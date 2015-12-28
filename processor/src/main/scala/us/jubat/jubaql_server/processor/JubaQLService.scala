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
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._

import com.twitter.finagle.Service
import com.twitter.util.{Future => TwFuture, Promise => TwPromise}
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.netty.util.CharsetUtil
import RunMode.{Production, Development}
import us.jubat.jubaql_server.processor.json._
import us.jubat.jubaql_server.processor.updater._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkFiles, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Row}
import org.apache.spark.sql.catalyst.plans.logical.{Project, BinaryNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.json4s._
import org.json4s.native.{JsonMethods, Serialization}
import org.json4s.JsonDSL._
import org.apache.commons.io._
import sun.misc.Signal
import us.jubat.anomaly.AnomalyClient
import us.jubat.classifier.{ClassifierClient, LabeledDatum}
import us.jubat.common.{Datum, ClientBase}
import us.jubat.recommender.RecommenderClient
import us.jubat.yarn.client.{JubatusYarnApplication, JubatusYarnApplicationStatus, Resource, JubatusClusterConfiguration}
import us.jubat.yarn.common._

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.{LinkedHashMap, HashMap, ArrayBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await => ScAwait, Future => ScFuture, Promise => ScPromise, SyncVar}
import scala.util.{Failure, Random, Success, Try}

class JubaQLService(sc: SparkContext, runMode: RunMode, checkpointDir: String)
  extends Service[HttpRequest, HttpResponse]
  with LazyLogging {
  val random = new Random()
  val parser = new JubaQLParser()
  // alias name for parser is needed to override SQLContext's parser
  val sqlc = new JubaQLContext(sc, parser)

  sqlc.registerFunction("highestScoreLabel", (classes: List[Row]) => {
    // actually we have a List[(String, Double)], but we get a List[Row]
    if (classes.isEmpty)
      ""
    else {
      classes.maxBy(_.getDouble(1)).getString(0)
    }
  })

  val sources: concurrent.Map[String, (HybridProcessor, Option[StructType])] =
    new ConcurrentHashMap[String, (HybridProcessor, Option[StructType])]().asScala
  val models: concurrent.Map[String, (JubatusYarnApplication, CreateModel, LearningMachineType)] =
    new ConcurrentHashMap[String, (JubatusYarnApplication, CreateModel, LearningMachineType)]().asScala
  val startedJubatusInstances: concurrent.Map[String, (ScFuture[JubatusYarnApplication], CreateModel, LearningMachineType)] =
    new ConcurrentHashMap[String, (ScFuture[JubatusYarnApplication], CreateModel, LearningMachineType)]().asScala

  // hold all statements received from a client, together with the data source name
  // TODO replace this by a synchronized version?
  val preparedStatements: mutable.Queue[(String, PreparedJubaQLStatement)] = new mutable.Queue()

  // hold names of all usable table-like objects, mapping to their main data source name
  val knownStreamNames: concurrent.Map[String, String] =
    new ConcurrentHashMap[String, String]().asScala

  val streamStates: concurrent.Map[String, StreamState] =
    new ConcurrentHashMap[String, StreamState]().asScala

  // hold feature functions written in JavaScript.
  val featureFunctions: concurrent.Map[String, String] =
    new ConcurrentHashMap[String, String]().asScala

  val builtinFeatureFunctions = Set("id")

  val jubatusFeatureFunctions = Set("unigram", "bigram")

  // a feature function is invalid if it is not in one of the three possible sets
  def invalidFeatureFunctions(ffs: List[String]): Set[String] = {
    ffs.toSet.
      diff(featureFunctions.keySet).
      diff(builtinFeatureFunctions).
      diff(jubatusFeatureFunctions)
  }

  // set this flag to `false` to prevent the HTTP server from processing queries
  protected val isAcceptingQueries: SyncVar[Boolean] = new SyncVar()
  isAcceptingQueries.put(true)

  // set this value which will be communicated to executors via /status poll
  protected val driverStatusMessage: SyncVar[String] = new SyncVar()
  driverStatusMessage.put("running")

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
        resp.setContent(ChannelBuffers.copiedBuffer(driverStatusMessage.get,
          CharsetUtil.UTF_8))
        TwFuture.value(resp)

      // if we get POSTed a statement, process it
      case "/jubaql" if request.getMethod == HttpMethod.POST =>
        val body = request.getContent.toString(CharsetUtil.UTF_8)
        logger.debug("[%s] request body: %s".format(requestId, body))

        // create an empty promise and create the processing pipeline
        val command = new TwPromise[String]
        val parsedCommand: TwFuture[Either[(Int, String), JubaQLAST]] =
          command.map(parseJson)
        val actionResult: TwFuture[Either[(Int, String), JubaQLResponse]] =
          parsedCommand.map(_.right.flatMap(takeAction))
        // now actually put the received command in the promise,
        //  triggering the processing
        command.setValue(body)

        // create an HttpResponse based on the result
        val responseFuture = actionResult.map(res => {
          implicit val formats = DefaultFormats
          // pick HTTP response code and render JSON body
          val (resp, bodyJson) = res match {
            case Left((httpStatusCode, errMsg)) =>
              // there was an error in some inner function
              logger.warn("error during query processing: " + errMsg)
              (new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.valueOf(httpStatusCode)),
                Serialization.write(ErrorMessage(errMsg)))
            case Right(result) =>
              // we got a result that we can render as JSON
              (new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK),
                Serialization.write(result))
          }
          // add header and body
          resp.addHeader("Content-Type", "application/json; charset=utf-8")
          resp.setContent(ChannelBuffers.copiedBuffer(bodyJson, CharsetUtil.UTF_8))
          logger.info("[%s] request processing complete => %s".format(requestId,
            resp.getStatus.getCode))
          resp
        })
        responseFuture

      // return 404 in any other case
      case _ =>
        logger.info("[%s] => 404".format(requestId))
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.NOT_FOUND)
        TwFuture.value(response)
    }
  }

  protected def parseJson(in: String): Either[(Int, String), JubaQLAST] = {
    // parse string and extract the "query" field
    JsonMethods.parseOpt(in).map(_ \ "query") match {
      case Some(JString(queryString)) =>
        try {
          parser.parse(queryString) match {
            case None =>
              val msg = s"unable to parse queryString '$queryString'"
              logger.error(msg)
              Left((400, msg))
            case Some(result) =>
              Right(result)
          }
        } catch {
          case e: Throwable =>
            Left((400, s"unable to parse queryString '$queryString': " + e.getMessage))
        }
      case Some(other) =>
        val msg = s"received JSON '$in' did not contain a query string"
        logger.warn(msg)
        Left((400, msg))
      case None =>
        val msg = s"received string '$in' was not valid JSON"
        logger.warn(msg)
        Left((400, msg))
    }
  }

  // takes a JSON-shaped string describing a Jubatus config and adds a
  // default "converter" part if it is not present
  protected def complementInputJson(inputJsonString: String): Either[(Int, String), JObject] = {
    val defaultConverter = JObject(
      "converter" -> JObject(
        "num_filter_types" -> JObject(),
        "num_filter_rules" -> JArray(Nil),
        "string_filter_types" -> JObject(),
        "string_filter_rules" -> JArray(Nil),
        "num_types" -> JObject(),
        "num_rules" -> JArray(JObject("key" -> "*", "type" -> "num") :: Nil),
        // define two Jubatus-internal conversion methods
        "string_types" -> JObject("unigram" -> JObject("method" -> "ngram", "char_num" -> "1"),
          "bigram" -> JObject("method" -> "ngram", "char_num" -> "2")),
        "string_rules" -> JArray(
          // define rules how to recognize keys for internal conversion
          JObject("key" -> "*-unigram-jubaconv", "type" -> "unigram", "sample_weight" -> "tf", "global_weight" -> "bin") ::
          JObject("key" -> "*-bigram-jubaconv", "type" -> "bigram", "sample_weight" -> "tf", "global_weight" -> "bin") ::
          JObject("key" -> "*", "except" -> "*-jubaconv", "type" -> "str", "sample_weight" -> "tf", "global_weight" -> "bin") :: Nil)))

    JsonMethods.parseOpt(inputJsonString) match {
      case Some(obj: JObject) =>
        obj.values.get("converter") match {
          case None =>
            // if the input has no converter, then append the default one
            Right(obj ~ defaultConverter)
          case _ =>
            // if the input *does* have a converter, use it as is
            Right(obj)
        }

      case Some(_) =>
        Left((400, "input config is not a JSON object."))

      case None =>
        Left((400, "input config is not a JSON."))
    }
  }

  protected def complementResource(resourceJsonString: Option[String]): Either[(Int, String), Resource] = {

    resourceJsonString match {
      case Some(strResource) =>
        JsonMethods.parseOpt(strResource) match {
          case Some(obj: JObject) =>
            val masterMemory = checkConfigByInt(obj, "applicationmaster_memory", Resource.defaultMasterMemory, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val proxyMemory = checkConfigByInt(obj, "jubatus_proxy_memory", Resource.defaultJubatusProxyMemory, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val masterCores = checkConfigByInt(obj, "applicationmaster_cores", Resource.defaultMasterCores, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val containerPriority = checkConfigByInt(obj, "container_priority", Resource.defaultPriority, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val containerMemory = checkConfigByInt(obj, "container_memory", Resource.defaultContainerMemory, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val serverMemory = checkConfigByInt(obj, "jubatus_server_memory", Resource.defaultJubatusServerMemory, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val containerCores = checkConfigByInt(obj, "container_cores", Resource.defaultContainerCores, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val containerNodes = checkConfigByStringList(obj, "container_nodes") match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val containerRacks = checkConfigByStringList(obj, "container_racks") match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            Right(Resource(containerPriority, serverMemory, containerCores, masterMemory, proxyMemory, masterCores, containerMemory, containerNodes, containerRacks))

          case None =>
            Left((400, "Resource config is not a JSON"))
        }

      case None =>
        Right(Resource())
    }
  }

  protected def complementServerConfig(serverJsonString: Option[String]): Either[(Int, String), ServerConfig] = {

    serverJsonString match {
      case Some(strServer) =>
        JsonMethods.parseOpt(strServer) match {
          case Some(obj: JObject) =>
            val diffSet = obj.values.keySet diff Set("thread", "timeout", "mixer", "interval_sec", "interval_count", "zookeeper_timeout", "interconnect_timeout")
            if (diffSet.size != 0) {
              return Left(400, s"invalid server config elements (${diffSet.mkString(",")})")
            }

            val thread = checkConfigByInt(obj, "thread", ServerConfig.defaultThread, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val timeout = checkConfigByInt(obj, "timeout", ServerConfig.defaultTimeout, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val mixer = checkConfigByString(obj, "mixer", ServerConfig.defaultMixer.name) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                try {
                  Mixer.valueOf(value)
                } catch {
                  case e: Throwable =>
                    return Left(400, s"invalid mixer specify 'linear_mixer' or 'random_mixer' or 'broadcast_mixer' or 'skip_mixer'")
                }
            }

            val intervalSec = checkConfigByInt(obj, "interval_sec", ServerConfig.defaultIntervalSec, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val intervalCount = checkConfigByInt(obj, "interval_count", ServerConfig.defaultIntervalCount, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val zookeeperTimeout = checkConfigByInt(obj, "zookeeper_timeout", ServerConfig.defaultZookeeperTimeout, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val interconnectTimeout = checkConfigByInt(obj, "interconnect_timeout", ServerConfig.defaultInterconnectTimeout, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            Right(ServerConfig(thread, timeout, mixer, intervalSec, intervalCount, zookeeperTimeout, interconnectTimeout))

          case None =>
            Left((400, "Server config is not a JSON"))
        }

      case None =>
        Right(ServerConfig())
    }
  }

  protected def complementProxyConfig(proxyJsonString: Option[String]): Either[(Int, String), ProxyConfig] = {

    proxyJsonString match {
      case Some(strProxy) =>
        JsonMethods.parseOpt(strProxy) match {
          case Some(obj: JObject) =>
            val diffSet = obj.values.keySet diff Set("thread", "timeout", "zookeeper_timeout", "interconnect_timeout", "pool_expire", "pool_size")
            if (diffSet.size != 0) {
              return Left(400, s"invalid proxy config elements (${diffSet.mkString(",")})")
            }

            val thread = checkConfigByInt(obj, "thread", ProxyConfig.defaultThread, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val timeout = checkConfigByInt(obj, "timeout", ProxyConfig.defaultTimeout, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val zookeeperTimeout = checkConfigByInt(obj, "zookeeper_timeout", ProxyConfig.defaultZookeeperTimeout, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val interconnectTimeout = checkConfigByInt(obj, "interconnect_timeout", ProxyConfig.defaultInterconnectTimeout, 1) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val poolExpire = checkConfigByInt(obj, "pool_expire", ProxyConfig.defaultPoolExpire, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            val poolSize = checkConfigByInt(obj, "pool_size", ProxyConfig.defaultPoolSize, 0) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))

              case Right(value) =>
                value
            }

            Right(ProxyConfig(thread, timeout, zookeeperTimeout, interconnectTimeout, poolExpire, poolSize))

          case None =>
            Left((400, "Proxy config is not a JSON"))
        }

      case None =>
        Right(ProxyConfig())
    }
  }

  protected def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] = {
    ast match {
      case anything if isAcceptingQueries.get == false =>
        val msg = s"received $anything while shutting down, not taking action"
        logger.warn(msg)
        Left((503, msg))

      case cd: CreateDatasource =>
        if (knownStreamNames.contains(cd.sourceName)) {
          val msg = "data source '%s' already exists".format(cd.sourceName)
          logger.warn(msg)
          Left((400, msg))
        } else {
          val processor = new HybridProcessor(sc, sqlc,
            cd.sinkStorage, cd.sinkStreams,
            runMode,
            checkpointDir)
          val maybeSchema = cd.columns match {
            case Nil =>
              None
            case cols =>
              Some(StructType(cols.map {
                case (colName, dataType) => {
                  StructField(colName, dataType.toLowerCase match {
                    case "numeric" => DoubleType
                    case "string" => StringType
                    case "boolean" => BooleanType
                    case _ => ???
                  }, nullable = false)
                }
              }))
          }
          // register this datasource internally so subsequent statements
          // can look it up
          sources.put(cd.sourceName, (processor, maybeSchema))
          // data sources "point" to themselves
          knownStreamNames += ((cd.sourceName, cd.sourceName))
          Right(StatementProcessed("CREATE DATASOURCE"))
        }

      case cm: CreateModel =>
        val jubaType: LearningMachineType = cm.algorithm match {
          case "CLASSIFIER" =>
            LearningMachineType.Classifier
          case "ANOMALY" =>
            LearningMachineType.Anomaly
          case "RECOMMENDER" =>
            LearningMachineType.Recommender
        }

        // check if all feature functions exist
        val badFFs = invalidFeatureFunctions(cm.featureExtraction.map(_._2))
        if (!badFFs.isEmpty) {
          val msg = "unknown feature functions: " + badFFs.mkString(", ")
          logger.warn(msg)
          return Left((400, msg))
        }

        val configJsonStr: String = complementInputJson(cm.configJson) match {
          case Left((errCode, errMsg)) =>
            return Left((errCode, errMsg))
          case Right(config) =>
            import JsonMethods._
            compact(render(config))
        }
        // TODO: location, resource
        val resource = complementResource(cm.resConfigJson) match {
          case Left((errCode, errMsg)) =>
            return Left((errCode, errMsg))
          case Right(value) =>
            value
        }

        val serverConfig = complementServerConfig(cm.serverConfigJson) match {
          case Left((errCode, errMsg)) =>
            return Left((errCode, errMsg))
          case Right(value) =>
            value
        }

        val proxyConfig = complementProxyConfig(cm.proxyConfigJson) match {
          case Left((errCode, errMsg)) =>
            return Left((errCode, errMsg))
          case Right(value) =>
            value
        }

        var message: String = ""
        val gatewayAddress = scala.util.Properties.propOrElse("jubaql.gateway.address","")
        val sessionId = scala.util.Properties.propOrElse("jubaql.processor.sessionId","")
        val applicationName = s"JubatusOnYarn:$gatewayAddress:$sessionId:${jubaType.name}:${cm.modelName}"

        val juba: ScFuture[JubatusYarnApplication] = runMode match {
          case RunMode.Production(zookeeper) =>
            val location = zookeeper.map {
              case (host, port) => Location(InetAddress.getByName(host), port)
            }
            val jubaClusterConfig = JubatusClusterConfiguration(cm.modelName, jubaType, location, configJsonStr, null, resource, 2, applicationName, serverConfig, proxyConfig)
            JubatusYarnApplication.start(jubaClusterConfig)
          case RunMode.Development =>
            if (cm.proxyConfigJson.isDefined) {
              message = "(proxy setting has been ignored in Development mode)"
            }
            LocalJubatusApplication.start(cm.modelName, jubaType, configJsonStr, serverConfig)
        }

        // we keep a reference to the started instance so we can always check its status
        // and wait for it to come up if necessary
        val startedInstance = ScPromise[JubatusYarnApplication]()
        startedJubatusInstances.put(cm.modelName, (startedInstance.future, cm, jubaType))
        juba onComplete {
          case Success(j) =>
            logger.info("CREATE MODEL succeeded")
            models.put(cm.modelName, (j, cm, jubaType))
            startedInstance.completeWith(juba)
          case Failure(t) =>
            logger.warn("CREATE MODEL failed: " + t.getMessage)
            t.printStackTrace()
            startedInstance.completeWith(juba)
        }
        Right(StatementProcessed(s"CREATE MODEL (started) $message"))

      case CreateStreamFromSelect(streamName, selectPlan) =>
        if (knownStreamNames.contains(streamName)) {
          val msg = s"stream '$streamName' already exists"
          logger.warn(msg)
          Left((400, msg))
        } else {
          val refStreams = selectPlan.children.flatMap(collectAllChildren)
          withStreams(refStreams)(mainDataSource => {
              // register this stream internally
              knownStreamNames += ((streamName, mainDataSource))
              streamStates += ((streamName, new StreamState(sc, refStreams.toList)))
              preparedStatements.enqueue((mainDataSource, PreparedCreateStreamFromSelect(streamName,
                selectPlan, refStreams.toList)))
              Right(StatementProcessed("CREATE STREAM"))
          })
        }

      case CreateStreamFromSlidingWindow(streamName, windowSize, slideInterval,
        windowType, source, funcSpecs, postCond) =>
        // pick the correct aggregate functions for the given aggregate list
        val checkedFuncSpecs = funcSpecs.map {
          case (funcName, params, alias) =>
            val maybeAggFun: Either[String, (SomeAggregateFunction[_])] = try {
              funcName.toLowerCase match {
                case "avg" =>
                  AggregateFunctions.checkAvgParams(params)
                case "stddev" =>
                  AggregateFunctions.checkStdDevParams(params)
                case "quantile" =>
                  AggregateFunctions.checkQuantileParams(params)
                case "linapprox" =>
                  AggregateFunctions.checkLinApproxParams(params)
                case "fourier" =>
                  AggregateFunctions.checkFourierParams(params)
                case "wavelet" =>
                  AggregateFunctions.checkWaveletParams(params)
                case "histogram" =>
                  AggregateFunctions.checkHistogramParams(params)
                case "concat" =>
                  AggregateFunctions.checkConcatParams(params)
                case "maxelem" =>
                  AggregateFunctions.checkMaxElemParams(params)
                case other =>
                  Left("unknown aggregation function: " + other)
              }
            } catch {
              case e: Throwable =>
                Left("error while checking " + funcName + ": " +
                  e.getMessage)
            }
          maybeAggFun match {
            case Left(msg) =>
              Left(msg)
            case Right(aggFun) =>
              Right((funcName, aggFun, alias))
          }
        }
        // check if we have any errors in the aggregate list
        val errors = checkedFuncSpecs.collect {
          case Left(msg) => msg
        }
        if (errors.size > 0) {
          val msg = "invalid parameter specification: " + errors.mkString(", ")
          logger.warn(msg)
          Left((400, msg))
        } else if (knownStreamNames.contains(streamName)) {
          val msg = s"stream '$streamName' already exists"
          logger.warn(msg)
          Left((400, msg))
        } else {
          val refStreams = source.children.flatMap(collectAllChildren)

          withStreams(refStreams)(mainDataSource => {
              // register this stream internally
              knownStreamNames += ((streamName, mainDataSource))
              streamStates += ((streamName, new StreamState(sc, refStreams.toList)))
              val flattenedFuncs = checkedFuncSpecs.collect{ case Right(x) => x }
              // build the schema that will result from this statement
              // (add one additional column with the window timestamp if the
              // window is timestamp-based)
              val typeInfo = flattenedFuncs.map(c => (c._1, c._2.outType, c._3))
              val schemaHead = if (windowType == "time")
                StructField("jubaql_timestamp", StringType, nullable = false) :: Nil
              else
                Nil
              val schema = StructType(schemaHead ++ typeInfo.zipWithIndex.map{
                case ((funcName, dataType, maybeAlias), idx) =>
                  // if there was an AS given in the statement, fine. if not,
                  // use the function name (or function name + dollar + index
                  // if the same function is used multiple times).
                  val alias = maybeAlias.getOrElse({
                    if (typeInfo.filter(f => f._3.isEmpty && f._1 == funcName).size > 1)
                      funcName + "$" + idx
                    else
                      funcName
                  })
                  StructField(alias, dataType, nullable = false)
              })
              // at this point, the `source` already has the pre-condition applied
              // and the correct columns selected. however, we still need to add the
              // right casts to Double/String.
              val headColumns = if (windowType == "time")
                Alias(Cast(UnresolvedAttribute("jubaql_timestamp"), StringType),
                  "key")() :: Nil
              else
                Nil
              val projectedSource = source.asInstanceOf[Project]
              val sourceWithCast = Project(headColumns ++
                projectedSource.projectList.zip(flattenedFuncs).map{
                  case (a: Alias, funcDesc) =>
                    Alias(Cast(a.child, funcDesc._2.inType), a.name)()
                  case (other, funcDesc) =>
                    Alias(Cast(other, funcDesc._2.inType), other.name)()
                }, projectedSource.child)
              val functionObjects = flattenedFuncs.map(_._2)
              preparedStatements.enqueue((mainDataSource, PreparedCreateStreamFromSlidingWindow(streamName,
                windowSize, slideInterval, windowType, sourceWithCast, functionObjects,
                schema, postCond)))
              Right(StatementProcessed("CREATE STREAM"))
            })
        }

      case cs: CreateStreamFromAnalyze =>
        val validCombination: (LearningMachineType, String) => Boolean = {
          case (LearningMachineType.Anomaly, "calc_score") => true
          case (LearningMachineType.Classifier, "classify") => true
          case (LearningMachineType.Recommender, "complete_row_from_id") => true
          case (LearningMachineType.Recommender, "complete_row_from_datum") => true
          case _ => false
        }
        if (knownStreamNames.contains(cs.streamName)) {
          val msg = "stream '%s' already exists".format(cs.streamName)
          logger.warn(msg)
          Left((400, msg))
        } else {
          withStream(cs.analyze.data)(mainDataSource => {
            prepareJubaClient(cs.analyze.modelName, cs.analyze.data, cs.analyze.rpcName,
              validCombination) match {
              case Right((modelFut, analyzerFut)) =>
                // register this stream internally
                knownStreamNames += ((cs.streamName, mainDataSource))
                streamStates += ((cs.streamName, new StreamState(sc, List(cs.analyze.data))))
                // put the UPDATE statement in the statement queue
                preparedStatements.enqueue((mainDataSource, PreparedCreateStreamFromAnalyze(cs.streamName,
                  cs.analyze.modelName, modelFut,
                  cs.analyze.data, analyzerFut, cs.analyze.rpcName,
                  cs.newColumn)))
                Right(StatementProcessed("CREATE STREAM"))
              case Left((code, msg)) =>
                Left((code, msg))
            }
          })
        }

      case CreateTrigger(dsName, condition, function) =>
        function match {
          case f: UnresolvedFunction =>
            JavaScriptUDFManager.getNumberOfArgsByFunctionName(f.name) match {
              case None =>
                val msg = s"no user-defined function named ${f.name}"
                logger.error(msg)
                return Left((400, msg))

              case Some(nargs) if nargs != f.children.size =>
                val msg = s"number of arguments is mismatched (number of arguments of ${f.name}} is ${f.children.size}})"
                logger.error(msg)
                return Left((400, msg))

              case _ =>
                // do nothing
            }
          case _ =>
            val msg = "unintentional Spark SQL builtin function"
            logger.error(msg)
            return Left((400, msg))
        }
        withStream(dsName)(mainDataSource => {
          preparedStatements.enqueue((mainDataSource,
            PreparedCreateTrigger(dsName, condition, function)))
          Right(StatementProcessed("CREATE TRIGGER"))
        })

      case LogStream(streamName) =>
        withStream(streamName)(mainDataSource => {
          preparedStatements.enqueue((mainDataSource, PreparedLogStream(streamName)))
          Right(StatementProcessed("LOG STREAM"))
        })

      case update: Update =>
        val validCombination: (LearningMachineType, String) => Boolean = {
          case (LearningMachineType.Anomaly, "add") => true
          case (LearningMachineType.Classifier, "train") => true
          case (LearningMachineType.Recommender, "update_row") => true
          case _ => false
        }
        withStream(update.source)(mainDataSource => {
          prepareJubaClient(update.modelName, update.source, update.rpcName,
            validCombination) match {
            case Right((modelFut, updaterFut)) =>
              // put the UPDATE statement in the statement queue
              preparedStatements.enqueue((mainDataSource, PreparedUpdate(update.modelName, modelFut,
                update.source, updaterFut)))
              Right(StatementProcessed("UPDATE MODEL"))
            case Left((code, msg)) =>
              Left((code, msg))
          }
        })

      case updateWith: UpdateWith =>
        queryUpdateWith(updateWith) match {
          case Left(msgWithErrCode) =>
            Left(msgWithErrCode)
          case Right(updateResult) =>
            Right(StatementProcessed(s"UPDATE MODEL ($updateResult)"))
        }

      case StartProcessing(sourceName) =>
        sources.get(sourceName) match {
          case None =>
            val msg = "unknown data source: " + sourceName
            logger.warn(msg)
            Left((400, msg))
          case Some((processor, _)) if processor.state != Initialized =>
            val msg = "cannot start processing a data source in state " + processor.state
            logger.warn(msg)
            Left((400, msg))
          case Some((processor, _)) if sources.values.exists(_._1.state == Running) =>
            val msg = "there is already a running process, try to run STOP PROCESSING first"
            logger.warn(msg)
            Left((400, msg))
          case Some((processor, maybeSchema)) =>
            logger.info(s"setting up processing pipeline for data source '$sourceName' " +
              s"with given schema $maybeSchema")

            val readyStreamList = ArrayBuffer.empty[String]
            val rddOperations: mutable.Queue[Either[(Int, String), StreamingContext => Unit]] =
              preparedStatements.filter(_._1 == sourceName).map(_._2).map(stmt => {
              logger.debug(s"deal with $stmt")
              stmt match {
                // CREATE STREAM ... FROM SELECT ...
                // => execute a select and register the result as a table
                case PreparedCreateStreamFromSelect(streamName, selectPlan, _) =>
                  logger.info(s"adding 'CREATE STREAM $streamName FROM SELECT ...' to pipeline")
                  Right((ssc: StreamingContext) => {
                    logger.debug(s"executing 'CREATE STREAM $streamName FROM SELECT ...'")
                    val selectedStream = SchemaDStream.fromSQL(ssc, sqlc, selectPlan, Some(streamName))
                    streamStates.get(streamName) match {
                      case Some(streamState) =>
                        readyStreamList += streamName
                        selectedStream.foreachRDD({ rdd =>
                          streamState.outputCount += rdd.count
                        })
                      case None =>
                        logger.warn(s"Stream(${streamName}) that counts the number of processing not found.")
                    }
                    ()
                  })

                // CREATE STREAM ... FROM SLIDING WINDOW ...
                case PreparedCreateStreamFromSlidingWindow(streamName, windowSize,
                  slideInterval, windowType, source, funcSpecs, outSchema, maybePostCond) =>
                  logger.info(s"adding 'CREATE STREAM $streamName FROM SLIDING WINDOW ...' to pipeline")
                  val fun = (ssc: StreamingContext) => {
                    logger.debug(s"executing 'CREATE STREAM $streamName FROM SLIDING WINDOW ...'")
                    // NB. the precondition is already applied in the `source`
                    val inputStream = SchemaDStream.fromSQL(ssc, sqlc, source, None)
                    val rowStream = inputStream.dataStream
                    val schemaStream = inputStream.schemaStream

                    // compute window stream
                    val windowStream = if (windowType == "tuples") {
                      SlidingWindow.byCount(rowStream, windowSize, slideInterval)
                    } else {
                      // the first column is the timestamp by construction
                      val keyedRowStream = rowStream.map(row => {
                        (Helpers.parseTimestamp(row.getString(0)),
                          Row(row.tail: _*))
                      })
                      // compute window stream
                      SlidingWindow.byTimestamp(keyedRowStream,
                        windowSize, slideInterval)
                    }

                    // if we access the window stream more than once, cache it
                    if (funcSpecs.size > 1) {
                      windowStream.persist(StorageLevel.MEMORY_AND_DISK_SER)
                    }
                    // apply the i-th aggregate function on the i-th element
                    // of the selected row
                    val aggregatedStreams = funcSpecs.zipWithIndex.map{
                      case (f: DoubleInputAggFun, idx) =>
                        val doubleStream = windowStream.mapValues(rowWithKey =>
                          (rowWithKey._1, rowWithKey._2.getDouble(idx)))
                        doubleStream.transform(f.aggFun _)
                      case (f: StringInputAggFun, idx) =>
                        val stringStream = windowStream.mapValues(rowWithKey =>
                          (rowWithKey._1, rowWithKey._2.getString(idx)))
                        stringStream.transform(f.aggFun _)
                    }
                    // merge the aggregated columns together
                    val firstStream = aggregatedStreams.head.mapValues(_ :: Nil)
                    val combinedStream = aggregatedStreams.tail
                      .foldLeft(firstStream)((left, right) => {
                        left.join(right).mapValues(lr => lr._1 :+ lr._2)
                      }).transform(_.sortByKey())
                    // convert to Rows and add schema
                    val outRowStream = if (windowType == "tuples") {
                      combinedStream.map(keyVal => Row(keyVal._2 :_*))
                    } else {
                      combinedStream.map(keyVal => {
                        val data = Helpers.formatTimestamp(keyVal._1) :: keyVal._2
                        Row(data :_*)
                      })
                    }
                    val outSchemaCopy = outSchema // outSchema is not serializable
                    val outSchemaStream = schemaStream.map(_ => outSchemaCopy)
                    // apply the post condition ("HAVING") if present
                    val filteredOutRowStream = maybePostCond.map(postCond => {
                        outRowStream.transform(rdd => {
                          val schemaRdd = sqlc.applySchema(rdd, outSchemaCopy)
                          schemaRdd.where(postCond)
                        })
                      }).getOrElse(outRowStream)
                    val filteredOutRowWithSchemaStream = SchemaDStream(sqlc, filteredOutRowStream, outSchemaStream)
                    streamStates.get(streamName) match {
                      case Some(streamState) =>
                        readyStreamList += streamName
                        filteredOutRowWithSchemaStream.foreachRDD({ rdd =>
                          streamState.outputCount += rdd.count
                        })
                      case None =>
                        logger.warn(s"Stream(${streamName}) that counts the number of processing not found.")
                    }
                    filteredOutRowWithSchemaStream.registerStreamAsTable(streamName)
                    ()
                  }
                  Right(fun)

                // CREATE STREAM ... FROM ANALYZE ...
                // => run updater.analyze on each partition
                case PreparedCreateStreamFromAnalyze(streamName, modelName,
                    modelFut, dataSourceName, analyzerFut, rpcName, newColumn) =>
                  // wait until model is available (when Jubatus is started) or timeout
                  if (!modelFut.isCompleted) {
                    logger.debug("waiting for model %s to come up".format(modelName))
                  } else {
                    logger.debug("model %s is already up".format(modelName))
                  }
                  val maybeModel = Try(ScAwait.result(modelFut, 1.minute))
                  maybeModel match {
                    case Failure(t) =>
                      val msg = "model %s failed to start up: %s".format(
                        modelName, t.getMessage)
                      logger.error(msg)
                      Left((500, msg))

                    case Success(juba) =>
                      // wait until updater is ready or timeout
                      Try(ScAwait.result(analyzerFut, 1.minute)) match {
                        case Failure(t) =>
                          val msg = "cannot use model %s: %s".format(
                            modelName, t.getMessage)
                          logger.error(msg)
                          Left((500, msg))

                        case Success(updater) =>
                          val (host, port) = JubaQLProcessor.getListeningAddress
                          val statusUrl = "http://%s:%s/status".format(host.getHostAddress, port)

                          logger.info(s"adding 'CREATE STREAM $streamName FROM ANALYZE ...' to pipeline")
                          Right((ssc: StreamingContext) => {
                            logger.debug(s"executing 'CREATE STREAM $streamName FROM ANALYZE ...'")
                            val analyzedStream = SchemaDStream.fromRDDTransformation(ssc, sqlc, dataSourceName, tmpRdd => {
                              val rddSchema: StructType = tmpRdd.schema
                              val analyzeFun = UpdaterAnalyzeWrapper(rddSchema, statusUrl,
                                updater, rpcName)
                              val newSchema = StructType(rddSchema.fields :+
                                StructField(newColumn.getOrElse(rpcName),
                                  analyzeFun.dataType, nullable = false))
                              val newRdd = sqlc.applySchema(tmpRdd.mapPartitionsWithIndex((idx, iter) => {
                                val formatter = new SimpleDateFormat("HH:mm:ss.SSS")
                                val hostname = InetAddress.getLocalHost().getHostName()
                                println("%s @ %s [%s] DEBUG analyzing model from partition %d".format(
                                  formatter.format(new Date), hostname, Thread.currentThread().getName, idx))
                                iter
                              }).mapPartitions(analyzeFun.apply(_)),
                                newSchema)
                              newRdd
                              }, Some(streamName))
                            streamStates.get(streamName) match {
                              case Some(streamState) =>
                                readyStreamList += streamName
                                analyzedStream.foreachRDD({ rdd =>
                                  streamState.outputCount += rdd.count()
                                })
                              case None =>
                                logger.warn(s"Stream(${streamName}) that counts the number of processing not found.")
                            }
                            ()
                          })
                      }
                  }

                case PreparedCreateTrigger(dsName, condition, expr) =>
                  logger.info(s"adding 'CREATE TRIGGER $dsName' to pipeline")
                  Right((ssc: StreamingContext) => {
                    logger.debug(s"executing 'CREATE TRIGGER $dsName'")
                    SchemaDStream.fromTableName(ssc, sqlc, dsName).foreachRDD(rdd => {
                      val rddWithCondition = condition match {
                        case None =>
                          rdd
                        case Some(c) =>
                          rdd.where(c)
                      }
                      rddWithCondition.select(expr).collect() // count() does not work here.
                      ()
                    })
                  })

                case PreparedLogStream(streamName) =>
                  logger.info(s"adding 'LOG STREAM $streamName' to pipeline")
                  Right((ssc: StreamingContext) => {
                    SchemaDStream.fromTableName(ssc, sqlc, streamName).foreachRDD(rdd => {
                    logger.debug(s"executing 'LOG STREAM $streamName'")
                    val dataToPrint = rdd.take(101)
                    val hasMoreData = dataToPrint.size == 101
                    val ellipsis =
                      if (hasMoreData) "\n( ... more items ...)"
                      else ""
                    println("STREAM: " + streamName + "\n" +
                      rdd.schema.fields.map(sf =>
                        sf.name + " " + sf.dataType).mkString(" | ") + "\n" +
                        dataToPrint.take(100).map(row => row.mkString(" | ")).mkString("\n") +
                        ellipsis
                    )
                    })
                    ()
                  })

                // UPDATE MODEL ... USING ...
                // => run updater.apply on each partition
                case PreparedUpdate(modelName, modelFut, dataSourceName, updaterFut) =>
                  // wait until model is available (when Jubatus is started) or timeout
                  if (!modelFut.isCompleted) {
                    logger.debug("waiting for model %s to come up".format(modelName))
                  } else {
                    logger.debug("model %s is already up".format(modelName))
                  }
                  val maybeModel = Try(ScAwait.result(modelFut, 1.minute))
                  maybeModel match {
                    case Failure(t) =>
                      val msg = "model %s failed to start up: %s".format(
                        modelName, t.getMessage)
                      logger.error(msg)
                      Left((500, msg))

                    case Success(juba) =>
                      // wait until updater is ready or timeout
                      Try(ScAwait.result(updaterFut, 1.minute)) match {
                        case Failure(t) =>
                          val msg = "cannot update model %s: %s".format(
                            modelName, t.getMessage)
                          logger.error(msg)
                          Left((500, msg))

                        case Success(updater) =>
                          val (host, port) = JubaQLProcessor.getListeningAddress
                          val statusUrl = "http://%s:%s/status".format(host.getHostAddress, port)

                          logger.info(s"adding 'UPDATE MODEL $modelName ...' to pipeline")
                          Right((ssc: StreamingContext) => {
                            SchemaDStream.fromTableName(ssc, sqlc, dataSourceName).foreachRDD(tmpRdd => {
                            logger.debug(s"executing 'UPDATE MODEL $modelName ...'")
                            val rddSchema: StructType = tmpRdd.schema
                            val updateFun = UpdaterApplyWrapper(rddSchema, statusUrl, updater)
                            // NOTE: you can add sample(...) here to work only on a subset of the items
                            tmpRdd.mapPartitionsWithIndex((idx, iter) => {
                              val formatter = new SimpleDateFormat("HH:mm:ss.SSS")
                              val hostname = InetAddress.getLocalHost().getHostName()
                              println("%s @ %s [%s] DEBUG updating model with partition %d".format(
                                formatter.format(new Date), hostname, Thread.currentThread().getName, idx
                              ))
                              iter
                            }).foreachPartition(updateFun.apply)
                            })
                          })
                      }
                  }

                // unknown statement type
                case _ =>
                  ???
              }
            })
            logger.info("pipeline setup complete (%d items)".format(rddOperations.size))

            rddOperations.collectFirst{ case Left(errDesc) => errDesc } match {
              // there was an error during pipeline setup
              case Some((code, msg)) =>
                Left((code, msg))

              // there was no error, but also no instructions
              case None if rddOperations.isEmpty =>
                val msg = "there are no processing instructions"
                logger.warn(msg)
                Left((400, msg))

              // there was no error
              case None =>
                def transform: SchemaDStream => Unit = inputStream => {
                  inputStream.registerStreamAsTable(sourceName)
                  val context = inputStream.dataStream.context
                  rddOperations.collect{ case Right(fun) => fun }.foreach(_.apply(context))
                }
                logger.info("starting HybridProcessor with created pipeline")
                val stopFun = processor.startTableProcessingGeneral(transform,
                  maybeSchema, sourceName)._1
                stopUpdateFunc = Some(() => stopFun())
                // set start time to streamState
                readyStreamList.foreach { startedStreamName =>
                  streamStates.get(startedStreamName) match {
                    case Some(state) =>
                      state.startTime = System.currentTimeMillis()
                    case None =>
                      logger.warn(s"${startedStreamName} is undefined")
                  }
                }
                Right(StatementProcessed("START PROCESSING"))
            }
        }

      case ana: Analyze =>
        queryAnalyze(ana) match {
          case Left(msgWithErrCode) =>
            Left(msgWithErrCode)
          case Right(anaResult) =>
            Right(AnalyzeResultWrapper(anaResult))
        }

      case s: Status =>
        val dsStatus = getSourcesStatus()
        val jubaStatus = getModelsStatus()
        val proStatus = getProcessorStatus()
        val streamStatus = getStreamStatus()
        logger.debug(s"dataSourcesStatus: $dsStatus")
        logger.debug(s"modelsStatus: $jubaStatus")
        logger.debug(s"processorStatus: $proStatus")
        logger.debug(s"streamStatus: $streamStatus")
        Right(StatusResponse("STATUS", dsStatus, jubaStatus, proStatus, streamStatus))

      case s: Shutdown =>
        // first set a flag to stop further query processing
        isAcceptingQueries.set(false) // NB. put() has different semantics
        // stop stream processing
        val procStats = stopUpdateFunc match {
          case Some(func) =>
            Some(stopStreamProcessing(func, forShutdown = true))
          case _ =>
            logger.info("apparently there was no stream processing running")
            None
        }
        // Shut down all Jubatus instances. First, loop over all Jubatus instances
        // ever started, independent of complete (successful or failed) or still
        // starting:
        val stoppedJubaFut: Iterable[ScFuture[Unit]] = startedJubatusInstances.map {
          case (modelName, (jubaFut, _, _)) =>
            logger.debug(s"scheduling shutdown for model $modelName")
            // If the startup failed, no need to shutdown. For all non-failed
            // instances (still starting or started successfully), we schedule
            // a shutdown using map().
            jubaFut.map(juba => shutdownJubatus(modelName, juba))
        }
        // now convert a list of futures into a future of list and wait until completion
        logger.info("waiting for all Jubatus instances to shut down")
        ScAwait.ready(ScFuture.sequence(stoppedJubaFut), 1.minute)
        // send a KILL signal to us to trigger Spark and Finagle shutdown
        Signal.raise(new Signal("TERM"))
        procStats match {
          case Some((staticInfo, streamInfo)) =>
            Right(StatementProcessed("SHUTDOWN (processing time: %s ms/%s ms)".format(
              staticInfo.runtime, streamInfo.runtime)))
          case _ =>
            Right(StatementProcessed("SHUTDOWN"))
        }

      case sp: StopProcessing =>
        stopUpdateFunc match {
          case Some(func) =>
            val (staticInfo, streamInfo) = stopStreamProcessing(func, forShutdown = false)
            stopUpdateFunc = None
            Right(StatementProcessed("STOP PROCESSING (processing time: %s ms/%s ms)".format(
              staticInfo.runtime, streamInfo.runtime)))
          case _ =>
            val msg = "apparently there was no stream processing running"
            logger.warn(msg)
            Left((400, msg))
        }

      case CreateFunction(funcName, args, returnType, lang, body) =>
        // TODO: write log
        // TODO: pass all args
        if (!lang.equalsIgnoreCase("JavaScript"))
          return Left((400, "only JavaScript is supported"))
        if (args.isEmpty)
          return Left((400, "args should contain at least one element"))

        val argString = args.map(_._1).mkString(", ")
        val funcBody = s"function $funcName($argString) { $body }"
        // try to find bugs in the syntax early
        try {
          JavaScriptUDFManager.register(funcName, args.size, funcBody)
        } catch {
          case e: Throwable =>
            // TODO: better message
            return Left((400, e.getMessage))
        }

        val validTypes = "numeric" :: "string" :: "boolean" :: Nil
        args.length match {
          case n if n <= 0 =>
            Left((400, "number of arguments must be more than zero."))

          case _ if !validTypes.contains(returnType) =>
            Left((400, "bad return type"))

          // def nArgsString(nArgs: Int): String =
          //   (0 until nArgs).map(n => s"x$n").mkString(", ")
          //
          // def nParamsString(nParams: Int): String = {
          //   (0 until nParams).map(n => s"x$n: AnyRef").mkString(", ")
          // }
          //
          // def caseTypeString(sqlType: String, scalaType: String, nArgs: Int): String = {
          //   val args = nArgsString(nArgs)
          //   val params = nParamsString(nArgs)
          //   s"""case "$sqlType" =>
          //      |  sqlc.registerFunction(funcName, ($params) => {
          //      |    JavaScriptUDFManager.registerAndCall[$scalaType](funcName,
          //      |      $nArgs, funcBody, $args)
          //      |  })""".stripMargin
          // }
          //
          // def caseNArgs(nArgs: Int): String = {
          //   val numericCase = caseTypeString("numeric", "Double", nArgs).split("\n").map("    " + _).mkString("\n")
          //   val stringCase = caseTypeString("string", "String", nArgs).split("\n").map("    " + _).mkString("\n")
          //   val booleanCase = caseTypeString("boolean", "Boolean", nArgs).split("\n").map("    " + _).mkString("\n")
          //   s"""case $nArgs =>
          //      |  returnType match {
          //      |$numericCase
          //      |$stringCase
          //      |$booleanCase
          //      |  }
          //      |  Right(StatementProcessed("CREATE FUNCTION"))
          //      |""".stripMargin
          // }
          //
          // following cases are generated with the above script.
          case 1 =>
            returnType match {
              case "numeric" =>
                sqlc.registerFunction(funcName, (x0: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Double](funcName,
                    1, funcBody, x0)
                })
              case "string" =>
                sqlc.registerFunction(funcName, (x0: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[String](funcName,
                    1, funcBody, x0)
                })
              case "boolean" =>
                sqlc.registerFunction(funcName, (x0: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Boolean](funcName,
                    1, funcBody, x0)
                })
            }
            Right(StatementProcessed("CREATE FUNCTION"))

          case 2 =>
            returnType match {
              case "numeric" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Double](funcName,
                    2, funcBody, x0, x1)
                })
              case "string" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[String](funcName,
                    2, funcBody, x0, x1)
                })
              case "boolean" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Boolean](funcName,
                    2, funcBody, x0, x1)
                })
            }
            Right(StatementProcessed("CREATE FUNCTION"))

          case 3 =>
            returnType match {
              case "numeric" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Double](funcName,
                    3, funcBody, x0, x1, x2)
                })
              case "string" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[String](funcName,
                    3, funcBody, x0, x1, x2)
                })
              case "boolean" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Boolean](funcName,
                    3, funcBody, x0, x1, x2)
                })
            }
            Right(StatementProcessed("CREATE FUNCTION"))

          case 4 =>
            returnType match {
              case "numeric" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Double](funcName,
                    4, funcBody, x0, x1, x2, x3)
                })
              case "string" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[String](funcName,
                    4, funcBody, x0, x1, x2, x3)
                })
              case "boolean" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Boolean](funcName,
                    4, funcBody, x0, x1, x2, x3)
                })
            }
            Right(StatementProcessed("CREATE FUNCTION"))

          case 5 =>
            returnType match {
              case "numeric" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef, x4: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Double](funcName,
                    5, funcBody, x0, x1, x2, x3, x4)
                })
              case "string" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef, x4: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[String](funcName,
                    5, funcBody, x0, x1, x2, x3, x4)
                })
              case "boolean" =>
                sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef, x4: AnyRef) => {
                  JavaScriptUDFManager.registerAndCall[Boolean](funcName,
                    5, funcBody, x0, x1, x2, x3, x4)
                })
            }
            Right(StatementProcessed("CREATE FUNCTION"))

          case _ =>
            Left((400, "too many arguments"))
        }

      case CreateFeatureFunction(funcName, args, lang, body) =>
        if (!lang.equalsIgnoreCase("JavaScript")) {
          val msg = s"language $lang is not supported"
          logger.warn(msg)
          return Left((400, msg))
        }
        if (args.isEmpty) {
          val msg = s"a function shall have at least one element"
          logger.warn(msg)
          return Left((400, msg))
        }

        val argString = args.map(_._1).mkString(", ")
        val funcBody = s"function $funcName($argString) { $body }"
        // try to find bugs in the syntax early
        try {
          JavaScriptFeatureFunctionManager.register(funcName, args.size, funcBody)
        } catch {
          case e: Throwable =>
            val msg = f"the function has syntax error: ${e.getMessage}"
            logger.warn(msg)
            return Left((400, msg))
        }

        featureFunctions += (funcName -> funcBody)
        Right(StatementProcessed("CREATE FEATURE FUNCTION"))

      case CreateTriggerFunction(funcName, args, lang, body) =>
        // TODO: write log
        // TODO: pass all args
        if (!lang.equalsIgnoreCase("JavaScript")) {
          val msg = s"language $lang is not supported"
          logger.warn(msg)
          return Left((400, msg))
        }
        if (args.isEmpty) {
          val msg = s"a function shall have at least one element"
          logger.warn(msg)
          return Left((400, msg))
        }

        val argString = args.map(_._1).mkString(", ")
        val funcBody = s"function $funcName($argString) { $body }"
        // try to find bugs in the syntax early
        try {
          JavaScriptUDFManager.register(funcName, args.size, funcBody)
        } catch {
          case e: Throwable =>
            val msg = f"the function has syntax error: ${e.getMessage}"
            logger.warn(msg)
            return Left((400, msg))
        }

        args.length match {
          case 1 =>
            // Returns an Int value because registerFunction does not accept a function which returns Unit.
            // The Int value is not used.
            sqlc.registerFunction(funcName, (x0: AnyRef) => {
              JavaScriptUDFManager.registerAndOptionCall[Int](funcName,
                1, funcBody, x0).getOrElse(0)
            })
            Right(StatementProcessed("CREATE TRIGGER FUNCTION"))

          case 2 =>
            // Returns Int for the above reason.
            sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef) => {
              JavaScriptUDFManager.registerAndOptionCall[Int](funcName,
                2, funcBody, x0, x1).getOrElse(0)
            })
            Right(StatementProcessed("CREATE TRIGGER FUNCTION"))

          case 3 =>
            // Returns Int for the above reason.
            sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef) => {
              JavaScriptUDFManager.registerAndOptionCall[Int](funcName,
                3, funcBody, x0, x1, x2).getOrElse(0)
            })
            Right(StatementProcessed("CREATE TRIGGER FUNCTION"))

          case 4 =>
            // Returns Int for the above reason.
            sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef) => {
              JavaScriptUDFManager.registerAndOptionCall[Int](funcName,
                4, funcBody, x0, x1, x2, x3).getOrElse(0)
            })
            Right(StatementProcessed("CREATE TRIGGER FUNCTION"))

          case 5 =>
            // Returns Int for the above reason.
            sqlc.registerFunction(funcName, (x0: AnyRef, x1: AnyRef, x2: AnyRef, x3: AnyRef, x4: AnyRef) => {
              JavaScriptUDFManager.registerAndOptionCall[Int](funcName,
                5, funcBody, x0, x1, x2, x3, x4).getOrElse(0)
            })
            Right(StatementProcessed("CREATE TRIGGER FUNCTION"))

          case _ =>
            val msg = "too many arguments"
            logger.warn(msg)
            Left((400, msg))
        }

      case SaveModel(modelName, modelPath, modelId) =>
        models.get(modelName) match {
          case Some((jubaApp, createModelStmt, machineType)) =>
            val chkResult = runMode match {
              case RunMode.Production(zookeeper) =>
                modelPath.startsWith("hdfs://")
              case RunMode.Development =>
                modelPath.startsWith("file://")
            }

            if (chkResult) {
              val juba = jubaApp.saveModel(new org.apache.hadoop.fs.Path(modelPath), modelId)
              juba match {
                case Failure(t) =>
                  val msg = s"SAVE MODEL failed: ${t.getMessage}"
                  logger.error(msg, t)
                  Left((500, msg))

                case _ =>
                  Right(StatementProcessed("SAVE MODEL"))
              }
            } else {
              val msg = s"invalid model path ($modelPath)"
              logger.warn(msg)
              Left((400, msg))
            }

          case None =>
            val msg = s"model '$modelName' does not exist"
            logger.warn(msg)
            Left((400, msg))
        }

      case LoadModel(modelName, modelPath, modelId) =>
        models.get(modelName) match {
          case Some((jubaApp, createModelStmt, machineType)) =>
            val chkResult = runMode match {
              case RunMode.Production(zookeeper) =>
                modelPath.startsWith("hdfs://")
              case RunMode.Development =>
                modelPath.startsWith("file://")
            }

            if (chkResult) {
              val juba = jubaApp.loadModel(new org.apache.hadoop.fs.Path(modelPath), modelId)
              juba match {
                case Failure(t) =>
                  val msg = s"LOAD MODEL failed: ${t.getMessage}"
                  logger.error(msg, t)
                  Left((500, msg))

                case _ =>
                  Right(StatementProcessed("LOAD MODEL"))
              }
            } else {
              val msg = s"invalid model path ($modelPath)"
              logger.warn(msg)
              Left((400, msg))
            }

          case None =>
            val msg = s"model '$modelName' does not exist"
            logger.warn(msg)
            Left((400, msg))
        }

      case other =>
        val msg = s"no handler for $other"
        logger.error(msg)
        Left((500, msg))
    }
  }

  // collect all tables referenced in a statement
  protected def collectAllChildren(plan: LogicalPlan): Seq[String] = plan match {
    case un: UnaryNode =>
      collectAllChildren(un.child)
    case bn: BinaryNode =>
      bn.children.flatMap(collectAllChildren)
    case UnresolvedRelation(tableIdentifier, _) =>
      tableIdentifier
    case other =>
      Nil
  }

  protected def prepareJubaClient(modelName: String, sourceName: String, rpcName: String,
                                  validCombination: (LearningMachineType, String) => Boolean):
      Either[(Int, String), (ScFuture[JubatusYarnApplication], ScFuture[JubatusClient])] = {
    // check if the specified model exists (or at least, was started)
    startedJubatusInstances.get(modelName) match {
      // no such model was defined before
      case None =>
        val msg = "no model called '%s'".format(modelName)
        logger.info(msg)
        Left((400, msg))

      // a model was defined before
      case Some((jubaFut, cm, jubaType)) =>
        jubaFut.value match {
          // complete, but with failure
          case Some(Failure(t)) =>
            val msg = "model %s failed to start up".format(modelName)
            logger.error(msg)
            Left((500, msg))

          // not yet complete (but started) or succeeded
          case _ =>
            // check if the specified stream exists
            if (knownStreamNames.contains(sourceName)) {
              // we prepare an instance of Update that only needs host and port
              // of the proxy when Jubatus is ready
              val almostAnUpdater: Try[(String, Int) => JubatusClient] = Try({
                // set up a (host, port) => Updater function or throw an exception
                jubaType match {
                  case lmt@LearningMachineType.Anomaly
                    if validCombination(lmt, rpcName) =>
                    (jubaHost, jubaPort) =>
                      new Anomaly(jubaHost, jubaPort, cm, featureFunctions)

                  case lmt@LearningMachineType.Classifier
                    if validCombination(lmt, rpcName) =>
                    val label = cm.labelOrId match {
                      case Some(("label", value)) =>
                        value
                      case _ =>
                        val msg = "no label for datum specified"
                        throw new IllegalArgumentException(msg)
                    }
                    (jubaHost, jubaPort) =>
                      new Classifier(jubaHost, jubaPort, cm, featureFunctions, label)

                  case lmt@LearningMachineType.Recommender
                    if validCombination(lmt, rpcName) =>
                    val id = cm.labelOrId match {
                      case Some(("id", value)) =>
                        value
                      case _ =>
                        val msg = "no id for datum specified"
                        throw new IllegalArgumentException(msg)
                    }
                    (jubaHost, jubaPort) =>
                      new Recommender(jubaHost, jubaPort, cm, featureFunctions, id)

                  case otherAlgorithm =>
                    val msg = "'%s' is not a valid method for %s".format(
                      rpcName, otherAlgorithm
                    )
                    logger.warn(msg)
                    throw new IllegalArgumentException(msg)
                }
              })
              // if that was successful, schedule Updater creation when
              // Jubatus is ready
              almostAnUpdater match {
                case Success(jubaCreator) =>
                  val updaterFut: ScFuture[JubatusClient] = jubaFut.map(model => {
                    val jubaHost = model.jubatusProxy.hostAddress
                    val jubaPort = model.jubatusProxy.port
                    jubaCreator(jubaHost, jubaPort)
                  })
                  // return the futures of Jubatus and Updater
                  Right((jubaFut, updaterFut))
                case Failure(t) =>
                  t match {
                    case _: IllegalArgumentException =>
                      logger.warn(t.getMessage)
                      Left((400, t.getMessage))
                    case _ =>
                      val msg = "unable to create Updater: " + t.getMessage
                      logger.warn(msg)
                      Left((500, msg))
                  }

              }
            } else {
              val msg = "source '%s' not found".format(sourceName)
              logger.error(msg)
              Left((400, msg))
            }
        }
    }
  }

  protected def acceptsMoreStatements(dataSourceName: String): Boolean = {
    sources.get(dataSourceName).map(_._1.state == Initialized).getOrElse(false)
  }

  /**
   * Run a function after ensuring the referenced stream exists and comes from a
   * valid data source.
   */
  protected def withStream(inputStreamName: String)(handler: String =>
    Either[(Int, String), JubaQLResponse]): Either[(Int, String), JubaQLResponse] = {
    knownStreamNames.get(inputStreamName) match {
      case Some(inputDataSourceName) =>
        sources.get(inputDataSourceName) match {
          case Some((inputDataSource, _)) if inputDataSource.state == Initialized =>
            handler(inputDataSourceName)
          case Some(_) =>
            val msg = s"data source '$inputDataSourceName' cannot accept further statements"
            logger.warn(msg)
            Left((400, msg))
          case None =>
            val msg = "data source with name '%s' does not exist".format(inputDataSourceName)
            logger.error(msg)
            Left((500, msg))
        }
      case None =>
        val msg = "source '%s' not found".format(inputStreamName)
        logger.error(msg)
        Left((400, msg))
    }
  }

  /**
   * Run a function after ensuring all referenced streams exist and come from the
   * same valid data source.
   */
  protected def withStreams(inputStreamNames: Seq[String])(handler: String =>
    Either[(Int, String), JubaQLResponse]): Either[(Int, String), JubaQLResponse] = {
    // look up which data source each stream comes from
    val refDataSources = inputStreamNames.flatMap(knownStreamNames.get(_)).toSet

    // check if there are referenced streams that we don't know
    (inputStreamNames.filter(!knownStreamNames.contains(_)), refDataSources.toList) match {
      // all referenced streams are known and they come from just one data source
      case (Nil, mainDataSource :: Nil) if acceptsMoreStatements(mainDataSource) =>
        handler(mainDataSource)
      // data source is not in the correct state
      case (Nil, mainDataSource :: Nil) =>
        val msg = s"data source '$mainDataSource' cannot accept further statements"
        logger.warn(msg)
        Left((400, msg))
      // all referenced streams are known, but they reference multiple data sources
      case (Nil, other) =>
        val msg = "you cannot use streams from multiple different data sources in one statement"
        logger.warn(msg)
        Left((400, msg))
      // some referenced streams have not been seen before
      case (unknownStreams, _) =>
        val msg = "unknown streams: %s".format(unknownStreams.mkString(", "))
        logger.warn(msg)
        Left((400, msg))
    }
  }

  protected def stopStreamProcessing(stopFun: () => (ProcessingInformation, ProcessingInformation),
                                     forShutdown: Boolean):
  (ProcessingInformation, ProcessingInformation) = {
    logger.info("stopping stream processing")
    // tell executors they should stop their processing
    if (forShutdown) {
      driverStatusMessage.set("shutdown") // NB. put() has different semantics
    } else {
      driverStatusMessage.set("stop-and-poll") // NB. put() has different semantics
    }
    // the following call will block until processing is done completely
    val (staticInfo, streamInfo) = stopFun()
    logger.info("shut down successfully; processed %s/%s items".format(
      staticInfo.itemCount, streamInfo.itemCount
    ))
    // if we are not executing a SHUTDOWN command, but a STOP PROCESSING
    // command, we must reset state so that we can continue processing later
    if (!forShutdown) {
      driverStatusMessage.set("running") // NB. put() has different semantics
    }
    (staticInfo, streamInfo)
  }

  protected def shutdownJubatus(modelName: String, app: JubatusYarnApplication) = {
    logger.info(s"shutting down model: $modelName")
    try {
      // We have to wait here for the stop() call to complete. If we don't block
      // until it is done, the main application may exit and kill this thread
      // (this function is actually called from a future.map()) before Jubatus
      // is stopped completely.
      ScAwait.ready(app.stop(), 1 minute)
      logger.info(s"model $modelName shut down successfully")
    } catch {
      case e: Throwable =>
        logger.error(s"failed to shut down $modelName: " + e.getMessage)
    }
  }

  protected def queryAnalyze(ana: Analyze): Either[(Int, String), AnalyzeResult] = {
    // TODO remove duplicated functionality with JubatusClient
    def datumToJson(datum: Datum): DatumResult = {
      DatumResult(
        datum.getStringValues().asScala.map(v => (v.key, v.value)).toMap,
        datum.getNumValues().asScala.map(v => (v.key, v.value)).toMap
      )
    }
    models.get(ana.modelName) match {
      case Some((jubaApp, createModelStmt, machineType)) =>
        val host = jubaApp.jubatusProxy.hostAddress
        val port = jubaApp.jubatusProxy.port

        machineType match {
          case LearningMachineType.Anomaly if ana.rpcName == "calc_score" =>
            val datum = DatumExtractor.extract(createModelStmt, ana.data, featureFunctions, logger)
            val anomaly = new AnomalyClient(host, port, ana.modelName, 5)
            try {
              Right(AnomalyScore(anomaly.calcScore(datum)))
            } finally {
              anomaly.getClient.close()
            }

          case LearningMachineType.Classifier if ana.rpcName == "classify" =>
            val datum = DatumExtractor.extract(createModelStmt, ana.data, featureFunctions, logger)
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
                Right(retValue)
              } else {
                val msg = "got an empty result from classifier"
                logger.error(msg)
                Left((500, msg))
              }
            } finally {
              classifier.getClient.close()
            }

          case LearningMachineType.Recommender if ana.rpcName == "complete_row_from_id" =>
            val recommender = new RecommenderClient(host, port, ana.modelName, 5)
            try {
              val retDatum = datumToJson(recommender.completeRowFromId(ana.data))
              Right(retDatum)
            } finally {
              recommender.getClient().close()
            }

          case LearningMachineType.Recommender if ana.rpcName == "complete_row_from_datum" =>
            val datum = DatumExtractor.extract(createModelStmt, ana.data, featureFunctions, logger)
            val recommender = new RecommenderClient(host, port, ana.modelName, 5)
            try {
              val retDatum = datumToJson(recommender.completeRowFromDatum(datum))
              Right(retDatum)
            } finally {
              recommender.getClient.close()
            }

          case _ =>
            val msg = "cannot use model '%s' with method '%s'".format(ana.modelName, ana.rpcName)
            logger.warn(msg)
            Left((400, msg))
        }

      case None =>
        val msg = "model '%s' does not exist".format(ana.modelName)
        logger.warn(msg)
        Left((400, msg))
    }
  }

  protected def checkConfigByInt(resObj: JObject, strKey: String, defValue: Int, minValue: Int = 0): Either[(Int, String), Int] = {
    resObj.values.get(strKey) match {
      case Some(value) =>
        try {
          val numValue = value.asInstanceOf[Number]
          val intValue = numValue.intValue()
          if (intValue >= minValue && intValue <= Int.MaxValue) {
            Right(intValue)
          } else {
            Left((400, s"invalid ${strKey} specified in ${minValue} or more and ${Int.MaxValue} or less"))
          }
        } catch {
          case e: Throwable =>
            logger.error(e.getMessage(), e)
            Left(400, s"invalid config (${strKey})")
        }

      case None =>
        Right(defValue)
    }
  }

  protected def checkConfigByString(resObj: JObject, strKey: String, defValue: String): Either[(Int, String), String] = {
    resObj.values.get(strKey) match {
      case Some(value) =>
        try {
          Right(value.asInstanceOf[String])
        } catch {
          case e: Throwable =>
            logger.error(e.getMessage(), e)
            Left(400, s"invalid config (${strKey})")
        }

      case None =>
        Right(defValue)
    }
  }

  protected def checkConfigByStringList(resObj: JObject, strKey: String): Either[(Int, String), List[String]] = {
    resObj.values.get(strKey) match {
      case Some(value) =>
        try {
          Right(value.asInstanceOf[List[String]])
        } catch {
          case e: Throwable =>
            logger.error(e.getMessage(), e)
            Left(400, s"invalid config (${strKey})")
        }

      case None =>
        Right(null)
    }
  }

  protected def getSourcesStatus(): Map[String, Any] = {
    var sourceMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    sources.foreach {
      case (sourceName, (hybridProcessor, schema)) =>
        sourceMap.put(sourceName, hybridProcessor.getStatus())
    }
    sourceMap
  }

  protected def getModelsStatus(): Map[String, Any] = {
    var jubaStatus: LinkedHashMap[String, Any] = new LinkedHashMap()
    models.foreach {
      case (modelName, (jubaApp, createModel, jubaType)) =>
        var configMap: LinkedHashMap[String, Any] = new LinkedHashMap()
        configMap.put("jubatusConfig", createModel.configJson)
        configMap.put("resourceConfig", createModel.resConfigJson.getOrElse(""))
        configMap.put("serverConfig", createModel.serverConfigJson.getOrElse(""))
        configMap.put("proxyConfig", createModel.proxyConfigJson.getOrElse(""))

        var jubatusAppStatusMap: LinkedHashMap[String, Any] = new LinkedHashMap()
        val jubatusAppStatus = jubaApp.status

        var proxyStatus: Map[String, Map[String, String]] = new HashMap()
        if (jubatusAppStatus.jubatusProxy != null) {
          proxyStatus = jubatusAppStatus.jubatusProxy.asScala.mapValues(map => map.asScala)
        }
        var serversStatus: Map[String, Map[String, String]] = new HashMap()
        if (jubatusAppStatus.jubatusServers != null) {
          serversStatus = jubatusAppStatus.jubatusServers.asScala.mapValues(map => map.asScala)
        }
        val yarnAppStatus: LinkedHashMap[String, Any] = new LinkedHashMap()
        if (jubatusAppStatus.yarnApplication != null) {
          jubatusAppStatus.yarnApplication.foreach {
            case (key, value) =>
              if (key == "applicationReport") {
                yarnAppStatus.put(key, value.toString())
              } else {
                yarnAppStatus.put(key, value)
              }
          }
        }

        jubatusAppStatusMap.put("jubatusProxy", proxyStatus)
        jubatusAppStatusMap.put("jubatusServers", serversStatus)
        jubatusAppStatusMap.put("jubatusOnYarn", yarnAppStatus)

        var modelMap: LinkedHashMap[String, Any] = new LinkedHashMap()
        modelMap.put("learningMachineType", jubaType.name)
        modelMap.put("config", configMap)
        modelMap.put("jubatusYarnApplicationStatus", jubatusAppStatusMap)
        jubaStatus.put(modelName, modelMap)
    }
    jubaStatus
  }

  protected def getProcessorStatus(): Map[String, Any] = {
    val curTime = System.currentTimeMillis()
    val opTime = curTime - sc.startTime
    val runtime = Runtime.getRuntime()
    val usedMemory = runtime.totalMemory() - runtime.freeMemory()

    var proStatusMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    proStatusMap.put("applicationId", sc.applicationId)
    proStatusMap.put("startTime", sc.startTime)
    proStatusMap.put("currentTime", curTime)
    proStatusMap.put("opratingTime", opTime)
    proStatusMap.put("virtualMemory", runtime.totalMemory())
    proStatusMap.put("usedMemory", usedMemory)

    proStatusMap
  }

  def getStreamStatus(): Map[String, Map[String, Any]] = {
    // Map{key = streamName, value = Map{key = statusName, value = statusValue}}
    var streamStatusMap: Map[String, Map[String, Any]] = Map.empty[String, Map[String, Any]]
    streamStates.foreach(streamState => {
      val stateValue = streamState._2
      var totalInputCount = 0L
      // calculate input count of each stream
      stateValue.inputStreamList.foreach { inputStreamName =>
        // add output count of datasource to totalInputCount
        sources.get(inputStreamName) match {
          case Some(source) =>
            totalInputCount += source._1.storageCount.value
            totalInputCount += source._1.streamCount.value
          case None =>
            logger.warn(s"input datasource(${inputStreamName}) of stream(${streamState._1}) was not found.")
        }
        // add output count of user define stream to totalInputCount
        streamStates.get(inputStreamName) match {
          case Some(inputStreamState) =>
            totalInputCount += inputStreamState.outputCount.value
          case None =>
            logger.warn(s"input stream(${inputStreamName}) of stream(${streamState._1}) was not found.")
        }
      }
      stateValue.inputCount = totalInputCount
      val stateMap: scala.collection.mutable.Map[String, Any] = new scala.collection.mutable.LinkedHashMap[String, Any]
      stateMap.put("stream_start", stateValue.startTime)
      stateMap.put("input_count", totalInputCount)
      stateMap.put("output_count", stateValue.outputCount.value)
      streamStatusMap += streamState._1 -> stateMap
    })
    streamStatusMap
  }

  protected def queryUpdateWith(updateWith: UpdateWith): Either[(Int, String), String] = {
    models.get(updateWith.modelName) match {
      case Some((jubaApp, createModelStmt, machineType)) =>
        val host = jubaApp.jubatusProxy.hostAddress
        val port = jubaApp.jubatusProxy.port

        machineType match {
          case LearningMachineType.Anomaly if updateWith.rpcName == "add" =>
            val datum = DatumExtractor.extract(createModelStmt, updateWith.learningData, featureFunctions, logger)
            val anomaly = new AnomalyClient(host, port, updateWith.modelName, 5)
            try {
              val result = anomaly.add(datum)
              logger.info(s"anomaly.add result: ${result.toString()}")
              Right(result.toString())
            } finally {
              anomaly.getClient.close()
            }

          case LearningMachineType.Classifier if updateWith.rpcName == "train" =>
            val datum = DatumExtractor.extract(createModelStmt, updateWith.learningData, featureFunctions, logger)
            val label = createModelStmt.labelOrId match {
              case Some(("label", value)) =>
                value
              case _ =>
                val msg = s"no label for datum specified"
                logger.warn(msg)
                return Left((400, msg))
            }

            val labelValue = getItemValue(updateWith.learningData, label) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))
              case Right(value) =>
                value
            }

            val labelDatum = new LabeledDatum(labelValue, datum)
            val datumList = new java.util.LinkedList[LabeledDatum]()
            datumList.add(labelDatum)
            val classifier = new ClassifierClient(host, port, updateWith.modelName, 5)
            try {
              val result = classifier.train(datumList)
              logger.info(s"classifier.train result: ${result}")
              Right(result.toString())
            } finally {
              classifier.getClient.close()
            }

          case LearningMachineType.Recommender if updateWith.rpcName == "update_row" =>
            val datum = DatumExtractor.extract(createModelStmt, updateWith.learningData, featureFunctions, logger)
            val idName = createModelStmt.labelOrId match {
              case Some(("id", value)) =>
                value
              case _ =>
                val msg = s"no id for datum specified"
                logger.warn(msg)
                return Left((400, msg))
            }

            val idValue = getItemValue(updateWith.learningData, idName) match {
              case Left((errCode, errMsg)) =>
                return Left((errCode, errMsg))
              case Right(value) =>
                value
            }

            val recommender = new RecommenderClient(host, port, updateWith.modelName, 5)
            try {
              val result = recommender.updateRow(idValue, datum)
              logger.info(s"recommender.update_row result: ${result}")
              Right(result.toString())
            } finally {
              recommender.getClient.close()
            }

          case _ =>
            val msg = s"cannot use model '${updateWith.modelName}' with method '${updateWith.rpcName}'"
            logger.warn(msg)
            Left((400, msg))
        }

      case None =>
        val msg = s"model '${updateWith.modelName}' does not exist"
        logger.warn(msg)
        Left((400, msg))
    }
  }

  protected def getItemValue(dataJsonString: String, itemName: String): Either[(Int, String), String] = {
    JsonMethods.parseOpt(dataJsonString) match {
      case Some(obj: JObject) =>
        obj.values.get(itemName) match {
          case Some(value) =>
            Right(value.toString())

          case None =>
            val msg = s"the given schema ${dataJsonString} does not contain a column named '${itemName}'"
            logger.warn(msg)
            Left((400, msg))
        }

      case _ =>
        val msg = "data is not JSON."
        logger.warn(msg)
        Left((400, msg))
    }
  }
}

sealed trait RunMode

object RunMode {

  case class Production(zookeeper: List[(String, Int)]) extends RunMode

  case object Development extends RunMode

  case object Test extends RunMode
}

object LocalJubatusApplication extends LazyLogging {
  def start(aLearningMachineName: String,
            aLearningMachineType: LearningMachineType,
            aConfigString: String,
            aServerConfig: ServerConfig = ServerConfig()): scala.concurrent.Future[us.jubat.yarn.client.JubatusYarnApplication] = {
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
        val rpcPort = findAvailablePort()
        val command = new StringBuilder
        command.append(s"$jubaCmdName")
        command.append(s" -p $rpcPort")
        command.append(s" -f $namedPipePath")
        command.append(s" -c ${aServerConfig.thread}")
        command.append(s" -t ${aServerConfig.timeout}")
        command.append(s" -x ${aServerConfig.mixer.name}")
        command.append(s" -s ${aServerConfig.intervalSec}")
        command.append(s" -i ${aServerConfig.intervalCount}")
        command.append(s" -Z ${aServerConfig.zookeeperTimeout}")
        command.append(s" -I ${aServerConfig.interconnectTimeout}")
        logger.debug(s"command: ${command.result()}")
        val jubatusProcess = runtime.exec(command.result())
        handleSubProcessOutput(jubatusProcess.getInputStream, System.out, jubaCmdName)
        handleSubProcessOutput(jubatusProcess.getErrorStream, System.err, jubaCmdName)
        val namedPipeWriter = new java.io.PrintWriter(namedPipe)
        try {
          namedPipeWriter.write(aConfigString)
        } finally {
          namedPipeWriter.close()
        }

        new LocalJubatusApplication(jubatusProcess, aLearningMachineName, aLearningMachineType,
            jubaCmdName, rpcPort)
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

  protected def findAvailablePort(): Int = {
    // connect to ports until we fail to connect to one
    Stream.from(9199).filter(port => {
      try {
        val socket = new java.net.Socket("127.0.0.1", port)
        socket.close()
        false
      } catch {
        case e: java.net.ConnectException =>
          true
        case e: Throwable =>
          false
      }
    }).head
  }
}

// LocalJubatusApplication is not a JubatusYarnApplication, but extends JubatusYarnApplication for implementation.
class LocalJubatusApplication(jubatus: Process, name: String, aLearningMachineType: LearningMachineType, jubaCmdName: String, port: Int = 9199)
  extends JubatusYarnApplication(Location(InetAddress.getLocalHost, port), List(), null) {

  private val timeoutCount: Int = 180
  private val fileRe = """file://(.+)""".r

  override def status: JubatusYarnApplicationStatus = {
    logger.info("status LocalJubatusApplication")

    val strHost = jubatusProxy.hostAddress
    val strPort = jubatusProxy.port
    val client: ClientBase = aLearningMachineType match {
      case LearningMachineType.Anomaly =>
        new AnomalyClient(strHost, strPort, name, timeoutCount)

      case LearningMachineType.Classifier =>
        new ClassifierClient(strHost, strPort, name, timeoutCount)

      case LearningMachineType.Recommender =>
        new RecommenderClient(strHost, strPort, name, timeoutCount)
    }

    val stsMap: java.util.Map[String, java.util.Map[String, String]] = client.getStatus()
    JubatusYarnApplicationStatus(null, stsMap, null)
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
    logger.info(s"loadModel path: $aModelPathPrefix, modelId: $aModelId")

    val strHost = jubatusProxy.hostAddress
    val strPort = jubatusProxy.port

    val srcDir = aModelPathPrefix.toUri().toString() match {
      case fileRe(filepath) =>
        val realpath = if (filepath.startsWith("/")) {
          filepath
        } else {
          (new java.io.File(".")).getAbsolutePath + "/" + filepath
        }
        "file://" + realpath
    }
    logger.debug(s"convert srcDir: $srcDir")

    val localFileSystem = org.apache.hadoop.fs.FileSystem.getLocal(new Configuration())
    val srcDirectory = localFileSystem.pathToFile(new org.apache.hadoop.fs.Path(srcDir))
    val srcPath = new java.io.File(srcDirectory, aModelId)
    if (!srcPath.exists()) {
      val msg = s"model path does not exist ($srcPath)"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val srcFile = new java.io.File(srcPath, "0.jubatus")
    if (!srcFile.exists()) {
      val msg = s"model file does not exist ($srcFile)"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val client: ClientBase = aLearningMachineType match {
      case LearningMachineType.Anomaly =>
        new AnomalyClient(strHost, strPort, name, timeoutCount)

      case LearningMachineType.Classifier =>
        new ClassifierClient(strHost, strPort, name, timeoutCount)

      case LearningMachineType.Recommender =>
        new RecommenderClient(strHost, strPort, name, timeoutCount)
    }

    val stsMap: java.util.Map[String, java.util.Map[String, String]] = client.getStatus()
    logger.debug(s"getStatus method result: $stsMap")
    if (stsMap.size != 1) {
      val msg = s"getStatus RPC failed (got ${stsMap.size} results)"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val strHostPort = stsMap.keys.head
    logger.debug(s"key[Host_Port]: $strHostPort")

    val baseDir = localFileSystem.pathToFile(new org.apache.hadoop.fs.Path(stsMap.get(strHostPort).get("datadir")))
    val mType = stsMap.get(strHostPort).get("type")
    val dstFile = new java.io.File(baseDir, s"${strHostPort}_${mType}_${aModelId}.jubatus")

    logger.debug(s"srcFile: $srcFile")
    logger.debug(s"dstFile: $dstFile")

    FileUtils.copyFile(srcFile, dstFile, false)

    val ret = client.load(aModelId)
    if (!ret) {
      val msg = "load RPC failed"
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    this
  }

  override def saveModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
    logger.info(s"saveModel path: $aModelPathPrefix, modelId: $aModelId")

    val strHost = jubatusProxy.hostAddress
    val strPort = jubatusProxy.port

    val strId = Math.abs(new Random().nextInt()).toString()

    val result: java.util.Map[String, String] = aLearningMachineType match {
      case LearningMachineType.Anomaly =>
        val anomaly = new AnomalyClient(strHost, strPort, name, timeoutCount)
        anomaly.save(strId)

      case LearningMachineType.Classifier =>
        val classifier = new ClassifierClient(strHost, strPort, name, timeoutCount)
        classifier.save(strId)

      case LearningMachineType.Recommender =>
        val recommender = new RecommenderClient(strHost, strPort, name, timeoutCount)
        recommender.save(strId)
    }

    logger.debug(s"save method result: $result")
    if (result.size != 1) {
      val msg = s"save RPC failed (got ${result.size} results)"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val strSavePath = result.values.head
    logger.debug(s"srcFile: $strSavePath")

    val dstDir = aModelPathPrefix.toUri().toString() match {
      case fileRe(filepath) =>
        val realpath = if (filepath.startsWith("/")) {
          filepath
        } else {
          s"${(new java.io.File(".")).getAbsolutePath}/$filepath"
        }
        s"file://$realpath"
    }
    logger.debug(s"convert dstDir: $dstDir")

    val localFileSystem = org.apache.hadoop.fs.FileSystem.getLocal(new Configuration())
    val dstDirectory = localFileSystem.pathToFile(new org.apache.hadoop.fs.Path(dstDir))
    val dstPath = new java.io.File(dstDirectory, aModelId)
    val dstFile = new java.io.File(dstPath, "0.jubatus")
    logger.debug(s"dstFile: $dstFile")

    if (!dstPath.exists()) {
      dstPath.mkdirs()
    } else {
      if (dstFile.exists()) {
        dstFile.delete()
      }
    }

    FileUtils.moveFile(new java.io.File(strSavePath), dstFile)
    this
  }
}

object TestJubatusApplication extends LazyLogging {
  def start(aLearningMachineName: String,
    aLearningMachineType: LearningMachineType): scala.concurrent.Future[us.jubat.yarn.client.JubatusYarnApplication] = {
    scala.concurrent.Future {
      new TestJubatusApplication(aLearningMachineName, aLearningMachineType)
    }
  }
}

class TestJubatusApplication(name: String, aLearningMachineType: LearningMachineType)
    extends JubatusYarnApplication(null, List(), null) {

  override def status: JubatusYarnApplicationStatus = {
    logger.info("status TestJubatusApplication")

    val dmyProxy: java.util.Map[String, java.util.Map[String, String]] = new java.util.HashMap()
    val dmyProxySub: java.util.Map[String, String] = new java.util.HashMap()
    dmyProxySub.put("PROGNAME", "jubaclassifier_proxy")
    dmyProxy.put("dummyProxy", dmyProxySub)
    val dmyServer: java.util.Map[String, java.util.Map[String, String]] = new java.util.HashMap()
    val dmyServerSub: java.util.Map[String, String] = new java.util.HashMap()
    dmyServerSub.put("PROGNAME", "jubaclassifier")
    dmyServer.put("dummyServer", dmyServerSub)
    val dmyApp: java.util.Map[String, Any] = new java.util.HashMap()
    dmyApp.put("applicationReport", "applicationId{ id: 1 cluster_timestamp: 99999999}")
    dmyApp.put("currentTime", System.currentTimeMillis())
    dmyApp.put("oparatingTime", 1000)
    JubatusYarnApplicationStatus(dmyProxy, dmyServer, dmyApp)
  }
}

class StreamState(sc: SparkContext, inputStreams: List[String]) {
  var inputCount = 0L
  val outputCount = sc.accumulator(0L)
  val inputStreamList = inputStreams
  var startTime = 0L
}