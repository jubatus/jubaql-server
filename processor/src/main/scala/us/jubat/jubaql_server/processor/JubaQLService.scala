// Jubatus: Online machine learning framework for distributed environment
// Copyright (C) 2014-2015 Preferred Networks and Nippon Telegraph and Telephone Corporation.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License version 2.1 as published by the Free Software Foundation.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
package us.jubat.jubaql_server.processor

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.Service
import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.netty.util.CharsetUtil
import us.jubat.jubaql_server.processor.json.{AnomalyScore, ClassifierPrediction, ClassifierResult, DatumResult}
import us.jubat.jubaql_server.processor.updater.{Anomaly, Classifier, Recommender}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.json4s._
import org.json4s.native.{JsonMethods, Serialization}
import us.jubat.anomaly.AnomalyClient
import us.jubat.classifier.ClassifierClient
import us.jubat.common.Datum
import us.jubat.recommender.RecommenderClient
import us.jubat.yarn.client.{JubatusYarnApplicationStatus, JubatusYarnApplication, Resource}
import us.jubat.yarn.common.{LearningMachineType, Location}

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{Future => ScFuture, Promise => ScPromise, Await => ScAwait, SyncVar}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success, Try}
import sun.misc.Signal

class JubaQLService(sc: SparkContext, runMode: RunMode)
  extends Service[HttpRequest, HttpResponse]
  with LazyLogging {
  val random = new Random()
  val parser = new JubaQLParser()
  // alias name for parser is needed to override SQLContext's parser
  val parserAlias = parser
  val sqlc = new SQLContext(sc) {
    override val parser = parserAlias
  }
  val sources: concurrent.Map[String, (HybridProcessor, StructType)] =
    new ConcurrentHashMap[String, (HybridProcessor, StructType)]().asScala
  val models: concurrent.Map[String, (JubatusYarnApplication, CreateModel, LearningMachineType)] =
    new ConcurrentHashMap[String, (JubatusYarnApplication, CreateModel, LearningMachineType)]().asScala
  val startedJubatusInstances: concurrent.Map[String, ScFuture[JubatusYarnApplication]] =
    new ConcurrentHashMap[String, ScFuture[JubatusYarnApplication]]().asScala

  // set this flag to `false` to prevent the HTTP server from processing queries
  protected val isAcceptingQueries: SyncVar[Boolean] = new SyncVar()
  isAcceptingQueries.put(true)

  // set this flag to `true` to signal to executors they should stop processing
  protected val executorsShouldFinishProcessing: SyncVar[Boolean] = new SyncVar()
  executorsShouldFinishProcessing.put(false)

  // store a function to stop the UPDATE process (if one is running)
  protected var stopUpdateFunc: Option[() => (ProcessingInformation, ProcessingInformation)] = None

  /** Sets up processing for an incoming request; returns Future of the result.
    */
  override def apply(request: HttpRequest): TwFuture[HttpResponse] = {
    /*
     * If no processing is required, a Future.value() can be used to
     * immediately return a value. Otherwise, create a Future with
     * the value to be processed (i.e., a Promise), create a processing
     * pipeline using map() and flatMap() and return a Future of the
     * response. Processing will then happen in a thread pool and newly
     * arriving requests will be enqueued.
     */
    val requestId = "req#" + Math.abs(random.nextInt)
    logger.info("[%s] received %s request to %s".format(requestId,
      request.getMethod.getName, request.getUri))

    request.getUri match {
      case "/status" =>
        val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.OK)
        if (executorsShouldFinishProcessing.get == true)
          resp.setContent(ChannelBuffers.copiedBuffer("shutdown", CharsetUtil.UTF_8))
        else
          resp.setContent(ChannelBuffers.copiedBuffer("running", CharsetUtil.UTF_8))
        TwFuture.value(resp)

      // if we get POSTed a statement, process it
      case "/jubaql" if request.getMethod == HttpMethod.POST =>
        val body = request.getContent.toString(CharsetUtil.UTF_8)
        logger.debug("[%s] request body: %s".format(requestId, body))

        // create an empty promise and create the processing pipeline
        val command = new TwPromise[String]
        // TODO: use Either or Future semantics to transport success/failure information
        val result: TwFuture[Option[String]] = command.map(parseJson).map(_.flatMap(takeAction))
        // now actually put the received command in the promise,
        //  triggering the processing
        command.setValue(body)

        // create an HttpResponse based on the result
        val responseFuture = result.map(res => {
          // pick HTTP response code and body
          val (resp, bodyJson) = res match {
            case Some(msg) =>
              (new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK),
                // msg may already be a JSON string
                // TODO: get this type-safe
                if (msg.startsWith("{") || msg.startsWith("["))
                  "{\"result\": %s}".format(msg)
                else
                  "{\"result\": \"%s\"}".format(msg))
            case _ =>
              (new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR),
                "{\"result\": \"error\"}")
          }
          // add header and body
          resp.addHeader("Content-Type", "application/json; charset=utf-8")
          resp.setContent(ChannelBuffers.copiedBuffer(bodyJson, CharsetUtil.UTF_8))
          logger.info("[%s] request processing complete => %s".format(requestId,
            resp.getStatus.getCode))
          resp
        })
        logger.debug("[%s] request processing prepared".format(requestId))
        responseFuture

      // return 404 in any other case
      case _ =>
        logger.info("[%s] => 404".format(requestId))
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.NOT_FOUND)
        TwFuture.value(response)
    }
  }

  protected def parseJson(in: String): Option[JubaQLAST] = {
    // parse string and extract the "query" field
    JsonMethods.parseOpt(in).map(_ \ "query") match {
      case Some(JString(queryString)) =>
        try {
          parser.parse(queryString)
        } catch {
          case e: Throwable =>
            logger.error(s"unable to parse queryString '$queryString': " + e.getMessage)
            None
        }
      case Some(other) =>
        logger.warn(s"received JSON '$in' did not contain a query string")
        None
      case None =>
        logger.warn(s"received string '$in' was not valid JSON")
        None
    }
  }

  protected def takeAction(ast: JubaQLAST): Option[String] = {
    ast match {
      case anything if isAcceptingQueries.get == false =>
        logger.warn(s"received $anything while shutting down, not taking action")
        // propagate message to client
        None

      case cd: CreateDatasource =>
        val processor = new HybridProcessor(sc, sqlc, cd.sinkStorage, cd.sinkStreams)
        // TODO schema must be optional
        val schema = StructType(cd.columns.map {
          case (colName, dataType) => {
            StructField(colName, dataType.toLowerCase match {
              case "numeric" => LongType
              case "string" => StringType
              case "boolean" => BooleanType
              case _ => ???
            }, false)
          }
        })
        sources.put(cd.sourceName, (processor, schema))
        Some("CREATE DATASOURCE")

      case cm: CreateModel =>
        val jubaType: LearningMachineType = cm.algorithm match {
          case "CLASSIFIER" =>
            LearningMachineType.Classifier
          case "ANOMALY" =>
            LearningMachineType.Anomaly
          case "RECOMMENDER" =>
            LearningMachineType.Recommender
        }
        // TODO: location, resource
        val resource = Resource(priority = 0, memory = 256, virtualCores = 1)
        val juba: ScFuture[JubatusYarnApplication] = runMode match {
          case RunMode.Production(zookeeper) =>
            val location = zookeeper.map {
              case (host, port) => Location(InetAddress.getByName(host), port)
            }
            JubatusYarnApplication.start(cm.modelName, jubaType, location, cm.configJson, resource, 2)
          case RunMode.Development =>
            LocalJubatusApplication.start(cm.modelName, jubaType, cm.configJson)
        }

        // we keep a reference to the started instance so we can always check its status
        // and wait for it to come up if necessary
        val startedInstance = ScPromise[JubatusYarnApplication]()
        startedJubatusInstances.put(cm.modelName, startedInstance.future)
        juba onComplete {
          case Success(j) =>
            logger.info("CREATE MODEL succeeded")
            models.put(cm.modelName, (j, cm, jubaType))
            startedInstance.completeWith(juba)
          case Failure(t) =>
            logger.info("CREATE MODEL failed")
            t.printStackTrace()
            startedInstance.completeWith(juba)
        }
        Some("CREATE MODEL (started)")

      case update: Update =>
        var model: JubatusYarnApplication = null
        var jubaType: LearningMachineType = null
        var cm: CreateModel = null
        // wait until model is available (when Jubatus is started) or timeout
        startedJubatusInstances.get(update.modelName).foreach(jubaFut => {
          if (!jubaFut.isCompleted) {
            logger.debug("waiting for model %s to come up".format(update.modelName))
            ScAwait.ready(jubaFut, 1 minute)
          }
        })
        val maybeModel = models.get(update.modelName)
        maybeModel match {
          case Some((s, c, ty)) => (s, c, ty)
            model = s
            cm = c
            jubaType = ty
          case None =>
            // TODO: error message
            logger.error("model not found")
            return None
        }

        // Note: Theoretically it would as well be possible to address the jubatus
        // instances directly by looking at `model.jubatusServers`.
        val jubaHost = model.jubatusProxy.hostAddress
        val jubaPort = model.jubatusProxy.port
        val trainSpecifier = cm.specifier.toMap
        val keys = trainSpecifier.get("datum") match {
          case Some(list) if list.nonEmpty => list
          case _ => ??? // TODO: throw exception. datum not specified
        }

        val updater = jubaType match {
          case LearningMachineType.Anomaly if update.rpcName == "add" =>
            new Anomaly(jubaHost, jubaPort, cm, keys)

          case LearningMachineType.Classifier if update.rpcName == "train" =>
            val label = trainSpecifier.get("label") match {
              case Some(la :: Nil) => la
              case _ => ??? // TODO: throw exception
            }
            new Classifier(jubaHost, jubaPort, cm, keys)

          case LearningMachineType.Recommender if update.rpcName == "update_row" =>
            val id = trainSpecifier.get("id") match {
              case Some(id :: Nil) => id
              case _ => ??? // TODO: throw exception
            }
            new Recommender(jubaHost, jubaPort, cm, id, keys)

          case lmt =>
            logger.error("no matching learning machine for " + lmt)
            return None
        }

        // Start to process RDD
        try sources.get(update.source) match {
          case Some((rddProcessor, schema)) =>
            logger.info("UPDATE started")
            val (host, port) = JubaQLProcessor.getListeningAddress
            val statusUrl = "http://%s:%s/status".format(host.getHostAddress, port)
            val stopFun = rddProcessor.start(rddjson => {
              rddjson.mapPartitions(updater(_, statusUrl))
            })._1
            // store the function to stop processing
            stopUpdateFunc = Some(() => stopFun())
            Some("UPDATE MODEL")

          case None =>
            // TODO: error message
            logger.error("source '%s' not found".format(update.source))
            None
        }

      case ana: Analyze =>
        queryAnalyze(ana) match {
          case Some(toReturn) =>
            Some(toReturn)
          case None =>
            logger.error("no ANALYZE result for " + ana)
            None
        }

      case s: Shutdown =>
        // first set a flag to stop further query processing
        isAcceptingQueries.set(false) // NB. put() has different semantics
      // stop stream processing
      val procStats = stopUpdateFunc match {
          case Some(func) =>
            Some(stopStreamProcessing(func))
          case _ =>
            logger.info("apparently there was no stream processing running")
            None
        }
        // Shut down all Jubatus instances. First, loop over all Jubatus instances
        // ever started, independent of complete (successful or failed) or still
        // starting:
        val stoppedJubaFut: Iterable[ScFuture[Unit]] = startedJubatusInstances.map {
          case (modelName, jubaFut) =>
            logger.debug(s"scheduling shutdown for model $modelName")
            // If the startup failed, no need to shutdown. For all non-failed
            // instances (still starting or started successfully), we schedule
            // a shutdown using map().
            jubaFut.map(juba => shutdownJubatus(modelName, juba))
        }
        // now convert a list of futures into a future of list and wait until completion
        logger.info("waiting for all Jubatus instances to shut down")
        ScAwait.ready(ScFuture.sequence(stoppedJubaFut), 1 minute)
        // send a KILL signal to us to trigger Spark and Finagle shutdown
        Signal.raise(new Signal("TERM"))
        procStats match {
          case Some((staticInfo, streamInfo)) =>
            Some("SHUTDOWN (processing time: %s ms/%s ms)".format(
              staticInfo.runtime, streamInfo.runtime))
          case _ =>
            Some("SHUTDOWN")
        }

      case sp: StopProcessing =>
        stopUpdateFunc match {
          case Some(func) =>
            val (staticInfo, streamInfo) = stopStreamProcessing(func)
            stopUpdateFunc = None
            Some("STOP PROCESSING (processing time: %s ms/%s ms)".format(
              staticInfo.runtime, streamInfo.runtime))
          case _ =>
            logger.warn("apparently there was no stream processing running")
            None
        }

      case other =>
        logger.error("no handler for " + other)
        None
    }
  }

  protected def stopStreamProcessing(stopFun: () => (ProcessingInformation, ProcessingInformation)):
    (ProcessingInformation, ProcessingInformation) = {
    logger.info("stopping stream processing")
    // tell executors they should stop their processing
    executorsShouldFinishProcessing.set(true) // NB. put() has different semantics
    // the following call will block until processing is done completely
    val (staticInfo, streamInfo) = stopFun()
    logger.info("shut down successfully; processed %s/%s items".format(
      staticInfo.itemCount, streamInfo.itemCount
    ))
    (staticInfo, streamInfo)
  }

  protected def shutdownJubatus(modelName: String, app: JubatusYarnApplication) = {
    logger.info(s"shutting down model: $modelName")
    try {
      app.stop()
      logger.info(s"model $modelName shut down successfully")
    } catch {
      case e: Throwable =>
        logger.error(s"failed to shut down $modelName: " + e.getMessage)
    }
  }

  protected def extractDatum(keys: List[String], data: String): Datum = {
    extractDatum(keys, JsonMethods.parse(data))
  }

  protected def extractDatum(keys: List[String], jvalue: JValue): Datum = {
    // filter unused filed
    val filtered = jvalue.filterField {
      case JField(key, _) => keys.indexOf(key) >= 0
      case _ => false
    }

    var datum = new Datum
    filtered.foreach({
      j =>
        val key = j._1
        j._2 match {
          case JInt(v) =>
            datum.addNumber(key, v.toDouble)
          case JDouble(v) =>
            datum.addNumber(key, v)
          case JString(v) =>
            datum.addString(key, v)
          case _ =>
        }
        j
    })
    return datum
  }


  protected def queryAnalyze(ana: Analyze): Option[String] = {
    def datumToJson(datum: Datum): DatumResult = {
      DatumResult(
        datum.getStringValues().asScala.map(v => (v.key, v.value)).toMap,
        datum.getNumValues().asScala.map(v => (v.key, v.value)).toMap
      )
    }
    models.get(ana.modelName) match {
      case Some((s, cm, LearningMachineType.Anomaly)) if ana.rpcName == "calc_score" =>
        val host = s.jubatusProxy.hostAddress
        val port = s.jubatusProxy.port
        val keys = cm.specifier.toMap.get("datum") match {
          case Some(list) if list.nonEmpty => list
          case _ => ??? // TODO: throw exception. datum not specified
        }
        var datum = extractDatum(keys, ana.data)
        val anomaly = new AnomalyClient(host, port, ana.modelName, 5)
        try {
          val score = AnomalyScore(anomaly.calcScore(datum))
          implicit val formats = DefaultFormats
          return Some(Serialization.write(score))
        } finally {
          anomaly.getClient.close()
        }

      case Some((s, cm, LearningMachineType.Classifier)) if ana.rpcName == "classify" =>
        val host = s.jubatusProxy.hostAddress
        val port = s.jubatusProxy.port
        val keys = cm.specifier.toMap.get("datum") match {
          case Some(list) if list.nonEmpty => list
          case _ => ??? // TODO: throw exception. datum not specified
        }
        var datum = extractDatum(keys, ana.data)
        val data = new java.util.LinkedList[Datum]()
        data.add(datum)
        val classifier = new ClassifierClient(host, port, ana.modelName, 5)
        try {
          val res = classifier.classify(data)
          if (res.size() >= 1) {
            // return in json format
            val retValue = ClassifierResult(res.get(0).asScala.map({
              f => ClassifierPrediction(f.label, f.score)
            }).toList)
            implicit val formats = DefaultFormats
            return Some(Serialization.write(retValue))
          } else {
            // TODO: return error in json
          }
        } finally {
          classifier.getClient().close()
        }
      case Some((s, cm, LearningMachineType.Recommender)) if (ana.rpcName == "complete_row_from_id" ||
        ana.rpcName == "complete_row_from_datum") =>
        val host = s.jubatusProxy.hostAddress
        val port = s.jubatusProxy.port
        ana.rpcName match {
          case "complete_row_from_id" =>
            val recommender = new RecommenderClient(host, port, ana.modelName, 5)
            try {
              val retDatum = datumToJson(recommender.completeRowFromId(ana.data))

              implicit val formats = DefaultFormats
              return Some(Serialization.write(retDatum))
            } finally {
              recommender.getClient().close()
            }

          case "complete_row_from_datum" =>
            val keys = cm.specifier.toMap.get("datum") match {
              case Some(list) if list.nonEmpty => list
              case _ => ??? // TODO: throw exception. datum not specified
            }
            var datum = extractDatum(keys, ana.data)
            val recommender = new RecommenderClient(host, port, ana.modelName, 5)

            try {
              val retDatum = datumToJson(recommender.completeRowFromDatum(datum))

              implicit val formats = DefaultFormats
              return Some(Serialization.write(retDatum))
            } finally {
              recommender.getClient().close()
            }
          case _ =>
        }
      case _ =>
        // error
        None
    }
    None
  }
}

sealed trait RunMode

object RunMode {
  case class Production(zookeeper: List[(String, Int)]) extends RunMode
  case object Development extends RunMode
}

object LocalJubatusApplication extends LazyLogging {
  def start(aLearningMachineName: String,
            aLearningMachineType: LearningMachineType,
            aConfigString: String): scala.concurrent.Future[us.jubat.yarn.client.JubatusYarnApplication] = {
    scala.concurrent.Future {
      val jubaCmdName = aLearningMachineType match {
        case LearningMachineType.Anomaly =>
          "jubaanomaly"
        case LearningMachineType.Classifier =>
          "jubaclassifier"
        case LearningMachineType.Recommender =>
          "jubarecommender"
      }

      logger.info(s"start LocalJubatusApplication (name: $aLearningMachineName, $jubaCmdName)")
      val namedPipePath = s"/tmp/${aLearningMachineName.trim}"
      val runtime = Runtime.getRuntime
      try {
        val exitStatus = mkfifo(namedPipePath, runtime)
        if (exitStatus != 0) {
          logger.error(f"failed to create a named pipe at $namedPipePath%s with exit status $exitStatus")
        }
      } catch {
        case e: java.io.IOException =>
          logger.error(s"failed to create a named pipe at $namedPipePath")
          System.exit(1)
      }

      val namedPipe = new java.io.File(namedPipePath)
      try {
        val jubatusProcess = runtime.exec(s"$jubaCmdName -f $namedPipePath")
        handleSubProcessOutput(jubatusProcess.getInputStream, System.out, jubaCmdName)
        handleSubProcessOutput(jubatusProcess.getErrorStream, System.err, jubaCmdName)
        val namedPipeWriter = new java.io.PrintWriter(namedPipe)
        try {
          namedPipeWriter.write(aConfigString)
        } finally {
          namedPipeWriter.close()
        }

        new LocalJubatusApplication(jubatusProcess, aLearningMachineName, jubaCmdName)
      } finally {
        namedPipe.delete()
      }
    }
  }

  def mkfifo(path: String, runtime: Runtime): Int = {
    val mkfifoProcess = runtime.exec(Array("mkfifo", path))
    mkfifoProcess.waitFor()
  }

  private def handleSubProcessOutput(in: java.io.InputStream,
                                     out: java.io.PrintStream,
                                     jubaCmdName: String) {
    import java.io._
    val thread = new Thread {
      override def run {
        val reader = new BufferedReader(new InputStreamReader(in))
        try {
          var line = reader.readLine()
          while (line != null) {
            out.println(s"[$jubaCmdName] $line")
            line = reader.readLine()
          }
        } catch {
          case e: IOException =>
            logger.warn(s"caught IOException in a subprocess handler: ${e.getMessage}")
        }
        // Never close out here.
      }
    }
    thread.setDaemon(true)
    thread.start()
  }
}

// LocalJubatusApplication is not a JubatusYarnApplication, but extends JubatusYarnApplication for implementation.
class LocalJubatusApplication(jubatus: Process, name: String, jubaCmdName: String)
  extends JubatusYarnApplication(Location(InetAddress.getLocalHost, 9199), List(), null) {

  override def status: JubatusYarnApplicationStatus = {
    throw new NotImplementedError("status is not implemented")
  }

  override def stop(): scala.concurrent.Future[Unit] = scala.concurrent.Future {
    logger.info(s"stop LocalJubatusApplication (name: $name, $jubaCmdName)")
    Thread.sleep(200) // This sleep prevents zombie jubatus
    jubatus.destroy()
    jubatus.waitFor()
  }

  override def kill() {
    throw new NotImplementedError("kill is not implemented")
  }

  override def loadModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
    throw new NotImplementedError("loadModel is not implemented")
  }

  override def saveModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
    throw new NotImplementedError("saveModel is not implemented")
  }
}
