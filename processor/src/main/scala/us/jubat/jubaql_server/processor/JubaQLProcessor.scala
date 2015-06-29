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

import scala.sys.process._
import java.net.InetAddress
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Duration, Time, Await}
import org.jboss.netty.handler.codec.http._
import sun.misc.{SignalHandler, Signal}

object JubaQLProcessor extends LazyLogging {
  protected var portHolder: Option[Int] = None

  /** Main function to start the JubaQL processor application. */
  def main(args: Array[String]) {
    // check run.mode property (default: development mode).
    val runModeString = scala.util.Properties.propOrElse("run.mode", "")
    val runMode: RunMode = runModeString match {
      case "" | "development" =>
        RunMode.Development
      case "production" =>
        // Require that zookeeper is given in production mode in
        // the form "host:port,host:port,...". If port is not given, take 2181.
        val zookeeperString = scala.util.Properties.propOrElse("jubaql.zookeeper", "")
        if (zookeeperString.trim.isEmpty) {
          logger.error("system property jubaql.zookeeper must be given " +
            "in production mode (comma-separated host:port list)")
          System.exit(1)
        }
        val hostPortRe = "^([a-zA-Z0-9.]*[a-zA-Z0-9])(:[0-9]+)?$".r
        val hosts: Seq[(String, Int)] = zookeeperString.split(',').map(_ match {
          case hostPortRe(host, portWithColon) =>
            if (portWithColon == null)
              (host, 2181)
            else
              (host, portWithColon.stripPrefix(":").toInt)
          case x =>
            logger.error(s"'$zookeeperString' is not a valid jubaql.zookeeper string")
            System.exit(1)
            null
        })
        RunMode.Production(hosts.toList)
      case other =>
        logger.error(s"bad run.mode property: $other")
        System.exit(1)
        RunMode.Development // for type of the match expression
    }
    logger.debug(s"Starting JubaQLProcessor in run mode $runMode")

    // checkpointDir for Spark
    val checkpointDir = scala.util.Properties.propOrElse("jubaql.checkpointdir", "")
    if (checkpointDir.trim.isEmpty) {
      logger.error("No jubaql.checkpointdir property")
      System.exit(1)
    }

    // When run through spark-submit, the Java system property "spark.master"
    // will contain the master passed to spark-submit and we *must* use the
    // same; otherwise use "local[3]".
    val master = scala.util.Properties.propOrElse("spark.master", "local[3]")

    // start Spark
    logger.info("JubaQLProcessor Spark backend starting")
    val sc = new SparkContext(master, "JubaQL Processor")

    // start HTTP interface
    val service: Service[HttpRequest, HttpResponse] = new JubaQLService(sc, runMode, checkpointDir)
    val errorHandler = new HandleExceptions
    logger.info("JubaQLProcessor HTTP server starting")
    val server = Http.serve(":*", errorHandler andThen service)
    var isRegistered = false

    val address = server.boundAddress
    if (!address.isInstanceOf[java.net.InetSocketAddress]) {
      logger.error("current implementation of finagle server does not provide boundAdress as InetSocketAddress")
      System.exit(1)
    }
    val inetAddress = address.asInstanceOf[java.net.InetSocketAddress]
    val port = inetAddress.getPort
    portHolder = Some(port)
    logger.info(s"HTTP server listening on port $port")

    // Create a helper to do (un)registration from gateway, if URL is given.
    val regHandler = args.headOption.map(new RegistrationHandler(_))

    val shutDownNicely = new SignalHandler() {
      def handle(sig: Signal) {
        logger.info("received signal, shutting down")
        // unregister before shutting down the server so that we will not
        // receive any queries from the gateway after server was stopped
        if (isRegistered) {
          unregister(regHandler)
          isRegistered = false
        }
        // close HTTP server only after a short timeout to finish requests
        // (otherwise sometimes the response to a SHUTDOWN command won't
        // arrive at the client)
        Await.result(server.close(Time.now + Duration.fromSeconds(5)))
      }
    }

    // Ctrl+C
    Signal.handle(new Signal("INT"), shutDownNicely)
    // kill
    Signal.handle(new Signal("TERM"), shutDownNicely)

    // register -- will exit(1) on failure!
    isRegistered = register(regHandler)

    // main loop
    Await.ready(server)
    logger.info("JubaQLProcessor HTTP server stopped")
    sc.stop()
    logger.info("JubaQLProcessor Spark backend stopped")

    // If server was stopped by a signal, unregistration was already done, so
    // isRegistered is false. Otherwise (which case is that??), unregister now.
    if (isRegistered)
      unregister(regHandler)

    logger.info("JubaQLProcessor shut down successfully")
    // If (and only if) we used dispatch to (un)register with an HTTP server, the
    // program will not exit here because there are still threads running.
    // Therefore we add a manual system exit as the last line.
    val THREAD_DEBUG = false
    if (THREAD_DEBUG) {
      val threadDesc = Thread.getAllStackTraces().keySet().toArray().map(tObj => {
        val t = tObj.asInstanceOf[Thread]
        t.toString + " (daemon: " + t.isDaemon + ", " + t.getState + ")"
      })
      logger.debug("threads still running:\n" + threadDesc.mkString("\n"))
    }
    System.exit(0)
  }

  def getListeningAddress: (InetAddress, Int) = {
    // InetAddress.getLocalHost will probably/maybe lead to unforeseeable
    // behavior with multiple interfaces. Instead we use the IP address
    // that our own hostname resolves into. (This is a requirement also
    // imposed by CDH, so we can assume it is a reasonable choice.)
    val host = InetAddress.getByName("hostname".!!.trim)
    portHolder match {
      case Some(port) =>
        (host, port)
      case _ =>
        throw new UninitializedError()
    }
  }

  protected def register(regHandler: Option[RegistrationHandler]): Boolean = {
    regHandler.map(r => {
      logger.info("registering with JubaQLGateway at " + r.registerUrl)
      r.register match {
        case Left(exception) =>
          exception match {
            case e: IllegalArgumentException =>
              logger.error("invalid URL provided: " + e.getMessage)
            case e: Throwable =>
              logger.error("registration failed: " + e.toString)
          }
          System.exit(1)
        case Right(_) =>
          logger.info("registered successfully")
      }
      true // we will only come here if registration was successful
    }).getOrElse(false)
  }

  protected def unregister(regHandler: Option[RegistrationHandler]) = {
    regHandler.foreach(r => {
      logger.info("unregistering from JubaQLGateway at " + r.registerUrl)
      r.unregister match {
        case Left(exception) =>
          logger.error("unregistration failed: " + exception.getMessage)
        case Right(_) =>
          logger.info("unregistered successfully")
      }
    })
  }
}
