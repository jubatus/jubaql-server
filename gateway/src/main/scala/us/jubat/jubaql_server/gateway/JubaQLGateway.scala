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
import scopt.OptionParser

object JubaQLGateway extends LazyLogging {
  val defaultPort = 9877

  /** Main function to start the JubaQL gateway application.
    */
  def main(args: Array[String]) {
    val maybeParsedOptions: Option[CommandlineOptions] = parseCommandlineOption(args)
    if (maybeParsedOptions.isEmpty) {
      System.exit(1)
    }
    val parsedOptions = maybeParsedOptions.get

    val ipAddress: String = parsedOptions.ip
    val port: Int = parsedOptions.port

    val gatewayId = parsedOptions.gatewayId match {
      case "" => s"${ipAddress}_${port}"
      case id => id
    }

    val persist: Boolean = parsedOptions.persist

    var envp: Array[String] = Array()
    var runMode: RunMode = RunMode.Development()
    val runModeProperty: String = System.getProperty("run.mode")
    val sparkJar = Option(System.getProperty("spark.yarn.jar"))
    val zookeeperString = scala.util.Properties.propOrElse("jubaql.zookeeper", "")
    val devModeRe = "development:([0-9]+)".r
    val prodModeRe = "production:([0-9]+):([0-9]+)".r
    runModeProperty match {
      case null | "" | "development" =>
        runMode = RunMode.Development()

      case devModeRe(numThreadsString) =>
        runMode = RunMode.Development(numThreadsString.toInt)

      case "production" =>
        runMode = RunMode.Production(zookeeperString, sparkJar = sparkJar)

      case prodModeRe(numExecutorsString, coresPerExecutorString) =>
        runMode = RunMode.Production(zookeeperString, numExecutorsString.toInt,
          coresPerExecutorString.toInt, sparkJar = sparkJar)

      case _ =>
        System.err.println("Bad run.mode property")
        System.exit(1)
    }

    runMode match {
      case p: RunMode.Production =>
        System.getenv("HADOOP_CONF_DIR") match {
          case null =>
            logger.warn("HADOOP_CONF_DIR not set, using default")
            // set HADOOP_CONF_DIR if there is no such environment variable
            envp = Array("HADOOP_CONF_DIR=/etc/hadoop/conf")
          case path =>
            envp = Array(s"HADOOP_CONF_DIR=$path")
        }
        // Require that zookeeper is given in production mode.
        // Syntax check must be done by JubaQLProcessor.
        if (zookeeperString.trim.isEmpty) {
          logger.error("system property jubaql.zookeeper must be given " +
            "in production mode (comma-separated host:port list)")
          System.exit(1)
        }
      case p: RunMode.Development =>
        // When persist was configured, Set system property jubaql.zookeeper.
        if (persist && zookeeperString.trim.isEmpty) {
          logger.error("system property jubaql.zookeeper must be given " +
            "with set persist flag (--persist)")
          System.exit(1)
        } else if (!persist && !zookeeperString.trim.isEmpty) {
          logger.warn("persist flag is not specified; jubaql.zookeeper is ignored")
        }
      case _ =>
      // don't set environment in other mode
    }
    logger.info("Starting in run mode %s".format(runMode))

    val sparkDistribution: String = getPropertyOrExitIfEmpty("spark.distribution")
    val fatjar: String = getPropertyOrExitIfEmpty("jubaql.processor.fatjar")
    val checkpointDir = getCheckpointDir(runMode)
    val plan = new GatewayPlan(ipAddress, port, envp, runMode,
                               sparkDistribution = sparkDistribution,
                               fatjar = fatjar,
                               checkpointDir = checkpointDir, gatewayId, persist)
    val nettyServer = unfiltered.netty.Server.http(port).plan(plan)
    logger.info("JubaQLGateway starting")
    nettyServer.run()
    plan.close()
    logger.info("JubaQLGateway shut down successfully")
  }

  def parseCommandlineOption(args: Array[String]): Option[CommandlineOptions] = {
    val parser = new OptionParser[CommandlineOptions]("JubaQLGateway") {
      opt[String]('i', "ip") required() valueName ("<ip>") action {
        (x, o) =>
          o.copy(ip = x)
      } text ("IP address")
      opt[Int]('p', "port") optional() valueName ("<port>") action {
        (x, o) =>
          o.copy(port = x)
      } validate {
        x =>
          if (x >= 1 && x <= 65535) success else failure("bad port number; port number n must be \"1 <= n <= 65535\"")
      } text (f"port (default: $defaultPort%d)")
      opt[String]('g', "gatewayID") optional() valueName ("<gatewayID>") action {
        (x, o) =>
          o.copy(gatewayId = x)
      } text ("Gateway ID (default: ip_port)")
      opt[Unit]("persist") optional() valueName ("<persist>") action {
        (x, o) =>
          o.copy(persist = true)
      } text ("session persist")
    }

    parser.parse(args, CommandlineOptions())
  }

  private def getPropertyOrExitIfEmpty(name: String): String = {
    val prop = scala.util.Properties.propOrElse(name, "")
    if (prop.trim.isEmpty) {
      System.err.println(s"No ${name} property")
      System.exit(1)
    }
    prop
  }

  private def getCheckpointDir(runMode: RunMode): String = {
    val dir = scala.util.Properties.propOrElse("jubaql.checkpointdir", "")
    if (dir.trim.isEmpty) {
      runMode match {
        case RunMode.Production(_, _, _, _) =>
          "hdfs:///tmp/spark"
        case RunMode.Development(_) =>
          "file:///tmp/spark"
      }
    } else {
      dir
    }
  }
}

case class CommandlineOptions(ip: String = "", port: Int = JubaQLGateway.defaultPort, gatewayId: String = "", persist: Boolean = false)
