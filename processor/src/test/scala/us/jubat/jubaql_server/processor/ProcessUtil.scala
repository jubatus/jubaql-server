package us.jubat.jubaql_server.processor

import scala.sys.process.{Process, ProcessBuilder}

object ProcessUtil {
  /**
   * Returns a ProcessBuilder with an environment variable for checkpointDir.
   */
  def commandToProcessBuilder(command: Seq[String]): ProcessBuilder = {
    Process(command, None, "JAVA_OPTS" -> "-Djubaql.checkpointdir=file:///tmp/spark")
  }
}
