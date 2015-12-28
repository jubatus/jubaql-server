package us.jubat.jubaql_server.processor

import scala.sys.process.{Process, ProcessBuilder}

object ProcessUtil {

  def commandToProcessBuilder(command: Seq[String], env: String = "-Djubaql.checkpointdir=file:///tmp/spark"): ProcessBuilder = {
    Process(command, None, "JAVA_OPTS" -> env)
  }
}
