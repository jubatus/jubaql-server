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
import scala.collection.mutable
import org.scalatest._
import java.nio.file.{Paths, Files}
import unfiltered.response._
import unfiltered.util.RunnableServer
import unfiltered.request._
import org.json4s.JsonAST.{JString, JInt, JValue}

/** Tests the correct behavior with respect to registration at the gateway.
  *
  * We want to run the application with parameters in a separate process
  * and capture the output and return code. We use the sbt-start-script
  * plugin <https://github.com/sbt/sbt-start-script> to run the application,
  * since running with `sbt run` leads to significant overhead and makes
  * dealing with output complicated.
  */
class RegistrationSpec extends FlatSpec with Matchers with MockServer {

  override protected def beforeAll(): Unit = {
    // if there is no script to start the application yet, generate it
    if (!Files.exists(Paths.get("start-script/run"))) {
      Seq("sbt", "start-script").!
    }
    super.beforeAll()
  }

  /**
   * Returns a ProcessLogger logging stdout/stderr to two StringBuffers.
   */
  protected def getProcessLogger(): (ProcessLogger, StringBuffer, StringBuffer) = {
    val stdoutBuffer = new StringBuffer()
    val stderrBuffer = new StringBuffer()
    val logger = ProcessLogger(line => {
      stdoutBuffer append line
      stdoutBuffer append "\n"
    },
      line => {
        stderrBuffer append line
        stderrBuffer append "\n"
      })
    (logger, stdoutBuffer, stderrBuffer)
  }

  import ProcessUtil.commandToProcessBuilder

  // First, check for invalid input

  "Passing an invalid string as URL" should "print an error and exit" taggedAs (LocalTest) in {
    val command = Seq("./start-script/run", "xyz")
    val pb = commandToProcessBuilder(command)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    val exitCode = pb ! logger
    // check exit code and console output
    exitCode shouldBe 1
    stdoutBuffer.toString should include("invalid URL provided")
  }

  "Passing an URL of a non-existing server" should "print an error and exit" in {
    // start the client and specify a server that does (probably) not exist
    val command = Seq("./start-script/run", "http://lameiq2elakliajdlawkidl.jp/hoge")
    val pb = commandToProcessBuilder(command)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    val exitCode = pb ! logger
    // check exit code and console output
    exitCode shouldBe 1
    stdoutBuffer.toString should include("registration failed: " +
      "java.net.ConnectException")
  }

  // Now, check for valid input

  // received JSON will be stored in those maps for later inspection
  val registerData: mutable.SynchronizedMap[String, JValue] =
    new mutable.HashMap[String, JValue]() with mutable.SynchronizedMap[String, JValue]
  val unregisterData: mutable.SynchronizedMap[String, JValue] =
    new mutable.HashMap[String, JValue]() with mutable.SynchronizedMap[String, JValue]

  // this server mocks the gateway
  protected val server: RunnableServer = {
    unfiltered.netty.Server.http(9877).plan(
      // define the server behavior
      unfiltered.netty.cycle.Planify {
        // return some result if a query is given with valid json
        case req@POST(Path(Seg("test" :: testId :: Nil))) =>
          val body = JsonBody(req)
          body match {
            case Some(bodyData) =>
              val JString(action) = bodyData \ "action"
              // store the received JValue in a map for examination
              if (action == "register") {
                registerData += (testId -> bodyData)
                Ok ~> ResponseString("")
              } else if (action == "unregister") {
                unregisterData += (testId -> bodyData)
                Ok ~> ResponseString("")
              } else {
                BadRequest ~> ResponseString("error")
              }
            case None =>
              InternalServerError ~> ResponseString("error")
          }
        case _ =>
          NotFound ~> ResponseString("404")
      })
  }

  "Passing the URL of a gateway-like server" should "register there" taggedAs (LocalTest) in {
    val command = Seq("./start-script/run", "http://localhost:9877/test/reg1")
    val pb = commandToProcessBuilder(command)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    // run the command
    val process = pb run logger
    var submittedJson: Option[JValue] = None
    var waitedTime = 0
    // wait until we receive data or reach timeout
    while (submittedJson.isEmpty && waitedTime < 20000) {
      submittedJson = registerData.get("reg1")
      Thread.sleep(200)
      waitedTime += 200
    }
    val exitCode = killSpawnedProgram(process)
    // check exit code and console output
    exitCode shouldBe 0
    stdoutBuffer.toString should include(" registered successfully")
    stdoutBuffer.toString should include("JubaQLProcessor shut down successfully\n")
    // check the JSON sent for registration purposes
    submittedJson should not be empty
    val json = submittedJson.get
    (json \ "ip") shouldBe a[JString]
    (json \ "port").getClass shouldBe JInt(1).getClass
  }

  it should "unregister after receiving SIGTERM" taggedAs (LocalTest) in {
    val command = Seq("./start-script/run", "http://localhost:9877/test/unreg1")
    val pb = commandToProcessBuilder(command)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    // run the command
    val process = pb run logger
    // wait until registration is complete
    var waitedTime = 0
    while (registerData.get("unreg1").isEmpty && waitedTime < 20000) {
      Thread.sleep(200)
      waitedTime += 200
    }
    val exitCode = killSpawnedProgram(process)
    // check exit code and console output
    exitCode shouldBe 0
    stdoutBuffer.toString should include("unregistered successfully")
    stdoutBuffer.toString should include("JubaQLProcessor shut down successfully\n")
    // check the JSON sent for registration purposes
    val submittedJson = unregisterData.get("unreg1")
    submittedJson should not be empty
  }

  private def killSpawnedProgram(process: Process): Int = {
    // get the PID of the currently running program by means of `ps` parsing
    val programs = ("ps x" #| "grep java").!!
    // on linux, ps x output looks like: "13239 ?        Sl     0:16 java [...]"
    // on Mac OS, it looks like:         "39905 s001  S+     0:08.50 /usr/bin/java [...]"
    val psRe = """ *([0-9]+) .+?:[0-9.]+ (.+)""".r
    // TODO is there a way to get child processes using pstree etc.?
    val spawnedProcesses = programs.split('\n').toList.collect {
      // parse `ps x` output
      case psRe(pid, cmd) => (pid, cmd)
    }.filter {
      // try to find the program that we spawned
      case (pid, cmd) => cmd.contains("processor/") && cmd.contains("/target/scala-2.10/classes:")
    }
    // send a SIGTERM to end the program
    spawnedProcesses.foreach {
      case (pid, cmd) =>
        s"kill $pid".!!
    }
    // now wait for exit
    process.exitValue()
  }
}
