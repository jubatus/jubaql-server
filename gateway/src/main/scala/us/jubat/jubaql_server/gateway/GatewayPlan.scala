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
package us.jubat.jubaql_server.gateway

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor
import unfiltered.response._
import unfiltered.request._
import unfiltered.netty.{cycle, ServerErrorResponse}
import us.jubat.jubaql_server.gateway.json._
import scala.collection.mutable
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write
import scala.util.{Try, Success, Failure}
import java.io._
import dispatch._
import dispatch.Defaults._
import java.util.jar.JarFile
import java.nio.file.{StandardCopyOption, Files}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.collection.Map

// A Netty plan using async IO
// cf. <http://unfiltered.databinder.net/Bindings+and+Servers.html>.
// If it is not sharable, then it can handle just one request; otherwise
// this object will be reused for many requests.
@io.netty.channel.ChannelHandler.Sharable
class GatewayPlan(ipAddress: String, port: Int,
                  envpForProcessor: Array[String], runMode: RunMode,
                  sparkDistribution: String, fatjar: String,
                  checkpointDir: String, gatewayId: String, persistSession: Boolean,
                  threads: Int, channelMemory: Long, totalMemory: Long)
  extends cycle.Plan
  /* With cycle.SynchronousExecution, there is a group of N (16?) threads
     (named "nioEventLoopGroup-5-*") that will process N requests in
     parallel, the rest will be queued for later execution. This can
     lead to timeouts, but will prevent too many threads running in parallel.
     The documentation says: "Evaluates the intent and its response function
     on an I/O worker thread. This is only appropriate if the intent is fully
     CPU-bound. If any thread-blocking I/O is required, use deferred
     execution."
     With cycle.ThreadPool, there is an unbounded thread pool. 500
     concurrent clients will probably kill the server.
     With cycle.DeferralExecutor, a ThreadPoolExecutor can be chosen that
     can limit the number of threads, e.g., by memory. (That is what
     MemoryAwareThreadPoolExecutor does. This does not exactly work as
     expected, though: The thread pool always keeps the same size; at least
     when tested with Thread.sleep(5000) as "blocking code".)
   */
  with cycle.DeferralExecutor with cycle.DeferredIntent
  // for error handling
  with ServerErrorResponse
  with LazyLogging {
  lazy val underlying = new MemoryAwareThreadPoolExecutor(threads, channelMemory, totalMemory)

  val Lineseparator = System.lineSeparator()
  val StateRunStr = "state: RUNNING"

  // SessionManager: holds session ids mapping to keys and host:port locations, respectively
  val sessionManager = try {
    persistSession match {
      case true => new SessionManager(gatewayId, new ZookeeperStore)
      case false => new SessionManager(gatewayId)
    }
  } catch {
    case e: Throwable => {
      logger.error("failed to start session manager", e)
      throw e
    }
  }

  /* When starting the processor using spark-submit, we rely on a certain
   * logging behavior. It seems like the log4j.xml file bundled with
   * the application jar is *not* used when using spark-submit, at least
   * not before the file bundled with Spark. To get around this, we create
   * a local copy of that log4j file and pass it as a parameter to
   * spark-submit.
   */
  val tmpLog4jPath: String = try {
    val jar = new JarFile(new File(fatjar))
    val log4jFile = jar.getEntry("log4j-spark-submit.xml")
    val log4jIs = jar.getInputStream(log4jFile)
    val tmpFile = File.createTempFile("log4j", ".xml")
    Files.copy(log4jIs, tmpFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    tmpFile.deleteOnExit()
    tmpFile.getAbsolutePath
  } catch {
    case e: Throwable =>
      logger.error("failed to create temporary log4j.xml copy: " + e.getMessage)
      throw e
  }
  logger.debug("extracted log4j-spark-submit.xml file to %s".format(tmpLog4jPath))

  val errorMsgContentType = ContentType("text/plain; charset=utf-8")

  val DefaultTimeout: Long = 60000
  val submitTimeout = try {
    val timeoutString: String = System.getProperty("jubaql.gateway.submitTimeout", DefaultTimeout.toString)
    val timeout = timeoutString.toLong
    if (timeout < 1) {
      throw new Exception(s"""jubaql.gateway.submitTimeout value must be "1 <= n <= ${Long.MaxValue}"""")
    }
    timeout
  } catch {
    case e: Exception =>
      logger.warn(s"failed get jubaql.gateway.submitTimeout property. Use default Timeout : ${DefaultTimeout}", e)
      DefaultTimeout
  }
  logger.debug(s"set spark-submit startingTimeout = $submitTimeout ms")

  implicit val formats = DefaultFormats

  private val statusLock = new AnyRef
  var queryTransferCount: Long = 0
  var queryReceivedCount: Long = 0
  val startTime = System.currentTimeMillis()

  def intent = {
    case req@POST(Path("/login")) =>
      val body = readAllFromReader(req.reader)
      val reqSource = req.remoteAddr
      logger.debug(f"received HTTP request at /login from $reqSource%s with body: $body%s")

      val maybeJson = org.json4s.native.JsonMethods.parseOpt(body)
      val maybeSessionId = maybeJson.flatMap(_.extractOpt[SessionId])
      maybeSessionId match {
        case Some(sessionId) =>
          // connect existing session
          val session = sessionManager.getSession(sessionId.session_id)
          session match {
            case Failure(t) =>
              InternalServerError ~> errorMsgContentType ~> ResponseString("Failed to get session")
            case Success(sessionInfo) =>
              sessionInfo match {
                case SessionState.NotFound =>
                  logger.warn("received a query JSON without a usable session_id")
                  Unauthorized ~> errorMsgContentType ~> ResponseString("Unknown session_id")
                case SessionState.Registering(key) =>
                  logger.warn(s"processor for session $key has not registered yet")
                  ServiceUnavailable ~> errorMsgContentType ~> ResponseString("This session has not been registered. Wait a second.")
                case SessionState.Ready(host, port, key) =>
                  logger.info(s"received login request for existing session (${sessionId}) from ${reqSource}")
                  val sessionIdJson = write(SessionId(sessionId.session_id))
                  Ok ~> errorMsgContentType ~> ResponseString(sessionIdJson)
              }
          }
        case None =>
          // connect new session
          val session = sessionManager.createNewSession()
          session match {
            case Failure(t) =>
              InternalServerError ~> errorMsgContentType ~> ResponseString("Failed to create session")
            case Success((sessionId, key)) =>
              val callbackUrl = composeCallbackUrl(ipAddress, port, key)
              val gatewayAddress = s"${ipAddress}:${port}"

              val runtime = Runtime.getRuntime
              val cmd = mutable.ArrayBuffer(f"$sparkDistribution%s/bin/spark-submit",
                "--class", "us.jubat.jubaql_server.processor.JubaQLProcessor",
                "--master", "", // set later
                "--conf", "", // set later
                "--conf", s"log4j.configuration=file:$tmpLog4jPath",
                "--name", s"JubaQLProcessor:$gatewayAddress:$sessionId",
                fatjar,
                callbackUrl)
              logger.info(f"starting Spark in run mode $runMode%s (session_id: $sessionId%s)")
              val divide = runMode match {
                case RunMode.Production(zookeeper, numExecutors, coresPerExecutor, sparkJar, sparkDriverMemory, sparkExecutorMemory) =>
                  cmd.update(4, "yarn-cluster") // --master
                  // When we run the processor on YARN, any options passed in with run.mode
                  // will be passed to the SparkSubmit class, not the the Spark driver. To
                  // get the run.mode passed one step further, we use the extraJavaOptions
                  // variable. It is important to NOT ADD ANY QUOTES HERE or they will be
                  // double-escaped on their way to the Spark driver and probably never end
                  // up there.
                  cmd.update(6, "spark.driver.extraJavaOptions=-Drun.mode=production " +
                    s"-Djubaql.zookeeper=$zookeeper " +
                    s"-Djubaql.checkpointdir=$checkpointDir "+
                    s"-Djubaql.gateway.address=$gatewayAddress " +
                    s"-Djubaql.processor.sessionId=$sessionId") // --conf
                  // also specify the location of the Spark jar file, if given
                  val sparkJarParams = sparkJar match {
                    case Some(url) => "--conf" :: s"spark.yarn.jar=$url" :: Nil
                    case _ => Nil
                  }
                  cmd.insertAll(9, "--num-executors" :: numExecutors.toString ::
                    "--executor-cores" :: coresPerExecutor.toString :: sparkJarParams)
                  if (sparkDriverMemory != None) {
                    cmd ++= mutable.ArrayBuffer("--driver-memory", sparkDriverMemory.get)
                  }
                  if (sparkExecutorMemory != None) {
                    cmd ++= mutable.ArrayBuffer("--executor-memory", sparkExecutorMemory.get)
                  }
                  logger.debug("executing: " + cmd.mkString(" "))
                  Try {
                    val maybeProcess = Try(runtime.exec(cmd.toArray, envpForProcessor))
                    maybeProcess match {
                      case Success(_) =>
                        maybeProcess.flatMap { process =>
                          // cache spark-submit.log
                          val logBuffer = new scala.collection.mutable.StringBuilder()

                          val sparkProcessFuture = Future {
                            // NB. which stream we have to use and whether the message we are
                            // waiting for actually appears, depends on the log4j.xml file
                            // bundled in the application jar...
                            val is: InputStream = process.getInputStream
                            val isr = new InputStreamReader(is)
                            val br = new BufferedReader(isr)
                            var line: String = br.readLine()
                            while (line != null && !line.trim.contains(StateRunStr)) {
                              logBuffer.append("\t" + line + Lineseparator)
                              line = br.readLine()
                            }
                            val isStateRun = (line != null && line.trim.contains(StateRunStr))
                            Try(process.exitValue) match {
                              case Failure(t) if t.isInstanceOf[IllegalThreadStateException] =>
                                if (isStateRun) {
                                  logger.debug("Succeed to spark-submit")
                                } else {
                                  val returnCode = process.waitFor
                                  throw new RuntimeException(s"Failed to finish process. returnCode: ${returnCode}")
                                }
                              case Failure(t) =>
                                throw new RuntimeException("Failed to spark-submit", t)
                              case Success(returnCode) =>
                                // process finished => abnormal
                                throw new RuntimeException(s"Failed to finish process. returnCode: ${returnCode}")
                            }
                          }
                          Try(Await.result(sparkProcessFuture, Duration(submitTimeout, MILLISECONDS))) match {
                            case Success(_) =>
                              //watch spark-submit starting process
                              handleSubProcess(process, sessionId)
                              Success(1)
                            case Failure(t) =>
                              //get standard error
                              val is: InputStream = process.getErrorStream
                              val isr = new InputStreamReader(is)
                              val br = new BufferedReader(isr)
                              try {
                                var line = ""
                                while ({ line = br.readLine(); line ne null }) {
                                  logBuffer.append("\t" + line + Lineseparator)
                                }
                              } finally {
                                br.close()
                              }
                              if (t.isInstanceOf[java.util.concurrent.TimeoutException]) {
                                logger.error(s"processor did not start within timeout period (${submitTimeout / 1000} seconds)${Lineseparator}spark-submit log : ${Lineseparator}" + logBuffer.toString())
                              } else {
                                logger.error(s"${t.getMessage}${Lineseparator}spark-submit log : ${Lineseparator}" + logBuffer.toString())
                              }
                              process.destroy()
                              Failure(t)
                          }
                        }
                      case Failure(e) =>
                        Failure(e)
                    }
                  }
                case RunMode.Development(numThreads) =>
                  cmd.update(4, s"local[$numThreads]") // --master
                  cmd.update(6, "run.mode=development") // --conf
                  cmd.insertAll(7, Seq("--conf", s"jubaql.checkpointdir=$checkpointDir"))
                  logger.debug("executing: " + cmd.mkString(" "))

                  Try {
                    val maybeProcess = Try(runtime.exec(cmd.toArray))
                    maybeProcess match {
                      case Success(_) =>
                        maybeProcess.flatMap { process =>
                          handleSubProcessOutput(process.getInputStream, System.out)
                          handleSubProcessOutput(process.getErrorStream, System.err)
                          Success(1)
                        }
                      case Failure(t) =>
                        Failure(t)
                    }
                  }
                case RunMode.Test =>
                  // do nothing in test mode.
                  Success(1)
              }
              divide match {
                case Success(result) =>
                  result match {
                    case Failure(e) =>
                      logger.error(e.getMessage, e)
                      InternalServerError ~> errorMsgContentType ~> ResponseString("Failed to start Spark\n")
                    case Success(_) =>
                      logger.info(f"started Spark with callback URL $callbackUrl%s")
                      val sessionIdJson = write(SessionId(sessionId))
                      Ok ~> errorMsgContentType ~> ResponseString(sessionIdJson)
                    case 1 =>
                      // test mode result
                      logger.debug(f"started Spark with callback URL $callbackUrl%s in run.mode=test")
                      val sessionIdJson = write(SessionId(sessionId))
                      Ok ~> errorMsgContentType ~> ResponseString(sessionIdJson)
                  }
                case Failure(e) =>
                  logger.error(e.getMessage, e)
                  InternalServerError ~> errorMsgContentType ~> ResponseString("Failed to start Spark\n")
              }
          }

      }

    case req@POST(Path("/jubaql")) =>
      // TODO: treat very long input
      statusLock.synchronized {
        queryReceivedCount += 1
        logger.debug(s"queryReceivedCount: $queryReceivedCount")
      }
      val body = readAllFromReader(req.reader)
      val reqSource = req.remoteAddr
      logger.debug(f"received HTTP request at /jubaql from $reqSource%s with body: $body%s")
      val maybeJson = org.json4s.native.JsonMethods.parseOpt(body)
      val maybeQuery = maybeJson.flatMap(_.extractOpt[Query])
      maybeQuery match {
        case None if maybeJson.isEmpty =>
          logger.warn("received query which is not a JSON")
          BadRequest ~> errorMsgContentType ~> ResponseString("Not JSON")
        case None =>
          logger.warn("received an unacceptable JSON query")
          BadRequest ~> errorMsgContentType ~> ResponseString("Unacceptable JSON")
        case Some(query) =>
          val session = sessionManager.getSession(query.session_id)
          session match {
            case Failure(t) =>
              InternalServerError ~> errorMsgContentType ~> ResponseString("Failed to get session")
            case Success(sessionInfo) =>
              sessionInfo match {
                case SessionState.NotFound =>
                  logger.warn("received a query JSON without a usable session_id")
                  Unauthorized ~> errorMsgContentType ~> ResponseString("Unknown session_id")
                case SessionState.Registering(key) =>
                  logger.warn(s"processor for session $key has not registered yet")
                  ServiceUnavailable ~> errorMsgContentType ~> ResponseString("This session has not been registered. Wait a second.")
                case SessionState.Ready(host, port, key) =>
                  val queryJson = write(QueryToProcessor(query.query)).toString

                  val url = :/(host, port) / "jubaql"
                  val req = Http((url.POST << queryJson) > (x => x))

                  logger.debug(f"forward query to processor ($host%s:$port%d)")
                  statusLock.synchronized {
                    queryTransferCount += 1
                    logger.debug(s"queryTransferCount: $queryTransferCount")
                  }
                  req.either.apply() match {
                    case Left(error) =>
                      logger.error("failed to send request to processor [" + error.getMessage + "]")
                      BadGateway ~> errorMsgContentType ~> ResponseString("Bad gateway")
                    case Right(result) =>
                      val statusCode = result.getStatusCode
                      val responseBody = result.getResponseBody
                      val contentType = Option(result.getContentType).getOrElse("text/plain; charset=utf-8")
                      logger.debug(f"got result from processor [$statusCode%d: $responseBody%s]")
                      Status(statusCode) ~> ContentType(contentType) ~> ResponseString(responseBody)
                  }
              }
          }
      }


    case req@POST(Path(Seg("registration" :: key :: Nil))) =>
      // parse JSON and extract into case class
      val maybeJson = JsonBody(req)
      val maybeRegister = maybeJson.flatMap(_.extractOpt[Register]).
        filter(_.action == "register")
      val maybeUnregister = maybeJson.flatMap(_.extractOpt[Unregister]).
        filter(_.action == "unregister")

      if (!maybeRegister.isEmpty) {
        logger.info(f"start registration (key: $key%s)")
      } else if (!maybeUnregister.isEmpty) {
        logger.info(f"start unregistration (key: $key%s)")
      } else {
        logger.info(f"start registration or unregistration (key: $key%s)")
      }

      if (maybeJson.isEmpty) {
        logger.warn("received query not in JSON format")
        BadRequest ~> errorMsgContentType ~> ResponseString("Not JSON")
      } else if (maybeRegister.isEmpty && maybeUnregister.isEmpty) {
        logger.warn("received unacceptable JSON query")
        BadRequest ~> errorMsgContentType ~> ResponseString("Unacceptable JSON")
      } else {
        if (!maybeRegister.isEmpty) { // register
          val register = maybeRegister.get
          val (ip, port) = (register.ip, register.port)
          logger.debug(f"registering $ip%s:$port%d")
          val result = sessionManager.attachProcessorToSession(ip, port, key)
          result match {
            case Failure(t) =>
              InternalServerError ~> errorMsgContentType ~> ResponseString(s"Failed to register key : ${key}")
            case Success(sessionId) =>
              logger.info(s"registered session. sessionId : $sessionId")
              Ok ~> errorMsgContentType ~> ResponseString("Successfully registered")
          }
        } else { // unregister
          logger.debug("unregistering")
          val result = sessionManager.deleteSessionByKey(key)
          result match {
            case Failure(t) =>
              InternalServerError ~> errorMsgContentType ~> ResponseString(s"Failed to unregister key : ${key}")
            case Success((sessionId, key)) =>
              if (sessionId != null) {
                logger.info(s"unregistered session. sessionId: ${sessionId}")
              } else {
                logger.info(s"already delete session. key: ${key}")
              }
              Ok ~> errorMsgContentType ~> ResponseString("Successfully unregistered")
          }
        }
      }

    case req@POST(Path("/status")) =>
      val reqSource = req.remoteAddr
      logger.debug(s"received HTTP request at /status from $reqSource")
      val stsMap = getGatewayStatus()
      val strStatus: String = write(GatewayStatus(stsMap))
      logger.debug(s"Response: $strStatus")
      Ok ~> errorMsgContentType ~> ResponseString(strStatus)

  }

  private def composeCallbackUrl(ip: String, port: Int, key: String): String = {
    f"http://$ip%s:$port%d/registration/$key%s"
  }

  private def handleSubProcessOutput(in: InputStream,
                                     out: PrintStream): Unit = {
    val thread = new SubProcessOutputHandlerThread(in, out, logger)
    thread.setDaemon(true)
    thread.start()
  }

  private def handleSubProcess(process: Process, sessionId: String): Unit = {
    val thread = new SubProcessHandlerThread(process, sessionId, this, logger)
    thread.setDaemon(true)
    thread.start()
  }

  private def readAllFromReader(reader: java.io.Reader):String = {
    val sb = new StringBuffer()
    val buffer = Array[Char](1024)
    var nread = reader.read(buffer)
    while (nread >= 0) {
      sb.append(buffer, 0, nread)
      nread = reader.read(buffer)
    }
    sb.toString
  }

  def close():Unit = {
    sessionManager.close()
  }

  private def getGatewayStatus(): Map[String, Any] = {
    val curTime = System.currentTimeMillis()
    val opTime = curTime - startTime
    val runtime = Runtime.getRuntime()
    val usedMemory = runtime.totalMemory() - runtime.freeMemory()

    var stsMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    stsMap.put("ipAddress", ipAddress)
    stsMap.put("port", port)
    stsMap.put("user", System.getProperty("user.name"))
    stsMap.put("pid", java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")(0))
    stsMap.put("sparkDistribution", sparkDistribution)
    stsMap.put("runMode", runMode.name)
    stsMap.put("zookeeper", scala.util.Properties.propOrElse("jubaql.zookeeper", ""))
    stsMap.put("sessionIds", sessionManager.session2key.values)
    stsMap.put("startTime", startTime)
    stsMap.put("currentTime", curTime)
    stsMap.put("oparatingTime", opTime)
    stsMap.put("maxMemory", runtime.maxMemory())
    stsMap.put("usedMemory", usedMemory)
    stsMap.put("queryTransferCount", queryTransferCount)
    stsMap.put("queryReceivedCount", queryReceivedCount)
    stsMap
  }
}

private class SubProcessOutputHandlerThread(in: InputStream,
                                            out: PrintStream,
                                            logger: com.typesafe.scalalogging.slf4j.Logger) extends Thread {
  override def run(): Unit = {
    val reader = new BufferedReader(new InputStreamReader(in))
    try {
      var line = reader.readLine()
      while (line != null) {
        out.println(f"[spark-submit] $line%s")
        line = reader.readLine()
      }
    } catch {
      case e: IOException =>
        logger.warn("caught IOException in subprocess handler", e)
        ()
    }
    // Never close out here.
  }
}

private class SubProcessHandlerThread(process: Process, sessionId: String, parent: GatewayPlan, logger: com.typesafe.scalalogging.slf4j.Logger) extends Thread {
  override def run(): Unit = {
    try {
      process.waitFor()
      Try(process.exitValue) match {
        case Success(returnCode) =>
          if (returnCode == 0) {
            logger.info("Finished spark-submit")
          } else {
            logger.error(f"Failed to spark-submit. returnCode: $returnCode%s")
          }
        case Failure(e) =>
          logger.error("Failed to spark-submit", e)
      }
    } catch {
      case e: Exception =>
        logger.error("caught Exception in spark-submit error handler", e)
    } finally {
      try {
        parent.sessionManager.getSession(sessionId) match {
          case Success(state) =>
            state match {
              case SessionState.Ready(_, _, _) | SessionState.Registering(_) =>
                parent.sessionManager.deleteSessionById(sessionId) match {
                  case Success((sessionId, key)) =>
                    logger.debug(s"Finished terminate process. sessionId: ${sessionId}")
                  case Failure(t) =>
                    throw t
                }
              case _ =>
                logger.debug(s"Terminate process is not required. sessionId: ${sessionId}")
            }
          case Failure(t) =>
            throw t
        }
      } catch {
        case e: Exception =>
          logger.error(s"Failed to terminate process. sessionId: ${sessionId}", e)
      }
      process.destroy()
    }
  }
}

sealed abstract class RunMode(val name: String)

object RunMode {
  case class Production(zookeeper: String, numExecutors: Int = 3, coresPerExecutor: Int = 2,
                        sparkJar: Option[String] = None, sparkDriverMemory: Option[String] = None,
                        sparkExecutorMemory: Option[String] = None) extends RunMode("Production")
  case class Development(numThreads: Int = 3) extends RunMode("Development")
  case object Test extends RunMode("Test")
}
