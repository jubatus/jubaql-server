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
package us.jubat.jubaql_server.processor.integration

import scala.sys.process._
import scala.io.Source
import org.scalatest._
import java.nio.file.{Paths, Files}
import dispatch._
import dispatch.Defaults._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.util.{Success, Failure, Try}
import us.jubat.jubaql_server.processor._
import us.jubat.jubaql_server.processor.json.ClassifierResult

/** Tests the correct behavior as viewed from the outside.
  */
trait ProcessorTestManager
  extends Suite
  with BeforeAndAfter
  with BeforeAndAfterAll {

  implicit val formats = org.json4s.DefaultFormats

  var process: Process = null
  var stdout: StringBuffer = null
  var sendJubaQL: String => Try[(Int, JValue)] = null

  before {
    val startResult = startProcessor()
    process = startResult._1
    stdout = startResult._2
    sendJubaQL = startResult._3
  }

  after {
    // all tests should send SHUTDOWN for cleanup, this is just
    // a fallback to avoid zombie processes "in case of"
    sendJubaQL("SHUTDOWN")
    process.destroy()
  }

  val goodCdStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
  val goodLsStmt = """LOG STREAM ds1"""
  val goodCsStmt = """CREATE STREAM ds2 FROM SELECT label FROM ds1 WHERE name = 'test'"""
  val goodCsfaStmt = """CREATE STREAM ds3 FROM ANALYZE ds1 BY MODEL test1 USING classify AS newcol"""
  val goodCmStmt = """CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'"""
  val goodUmStmt = """UPDATE MODEL test1 USING train FROM ds1"""
  val goodAStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test1 USING classify"""

  import ProcessUtil.commandToProcessBuilder

  override protected def beforeAll(): Unit = {
    // if there is no script to start the application yet, generate it
    if (!Files.exists(Paths.get("start-script/run"))) {
      val pb = commandToProcessBuilder(Seq("sbt", "start-script"))
      pb.!
    }
    super.beforeAll()
  }

  protected def waitUntilDone(dsName: String, waitMax: Long,
                              checkInterval: Long = 400) = {
    val start = System.currentTimeMillis()
    var now = System.currentTimeMillis()
    var state = "Running"
    while (now - start < waitMax && state != "Finished") {
      sendJubaQL("STATUS") match {
        case Success(x) =>
          x._2 \ "sources" \ dsName match {
            case JString(currentState) =>
              state = currentState
            case other =>
              // this is a syntax error in the response
              throw new RuntimeException("no information about data " +
                s"source '$dsName' in the response")
          }
        case Failure(f) =>
          // this is a failure in the HTTP statement
          throw f
      }
      Thread.sleep(checkInterval)
      now = System.currentTimeMillis()
    }
  }

  protected def startProcessor(): (Process,
    StringBuffer, String => Try[(Int, JValue)]) = {
    val command = Seq("./start-script/run")
    val pb = commandToProcessBuilder(command)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    val process = pb run logger
    val port = getServerPort(stdoutBuffer)
    (process, stdoutBuffer, sendJubaQLTo(port))
  }

  protected def startProcessor(env: String): (Process,
    StringBuffer, String => Try[(Int, JValue)]) = {
    val command = Seq("./start-script/run")
    val pb = commandToProcessBuilder(command, env)
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    val process = pb run logger
    val port = getServerPort(stdoutBuffer)
    (process, stdoutBuffer, sendJubaQLTo(port))
  }

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

  protected def getServerPort(stdout: StringBuffer): Int = {
    val portRe = "(?s).+listening on port ([0-9]+)\n".r
    var port = 0
    while (port == 0) {
      stdout.toString match {
        case portRe(loggedPort) =>
          port = loggedPort.toInt
        case _ =>
          Thread.sleep(100)
      }
    }
    port
  }

  protected def sendJubaQLTo(port: Int)(stmt: String): Try[(Int, JValue)] = {
    val url = :/("localhost", port) / "jubaql"
    val body = compact(render("query" -> stmt))
    Http(url << body).either.apply() match {
      case Left(error) =>
        Failure(error)
      case Right(response) =>
        Try {
          (response.getStatusCode,
            parse(response.getResponseBody("UTF-8")))
        }
    }
  }
}

class CreateDataSourceSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE DATASOURCE" should "return HTTP 200 on correct syntax" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if a data source with the same name already exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")
    // create the same data source again
    val cd2Result = sendJubaQL(goodCdStmt)
    cd2Result shouldBe a[Success[_]]
    cd2Result.get._1 shouldBe 400
    cd2Result.get._2 \ "result" shouldBe JString("data source 'ds1' already exists")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 200 if a data source with a different name exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")
    // create another data source
    val cd2Result = sendJubaQL(goodCdStmt.replace(" ds1", " ds2"))
    cd2Result shouldBe a[Success[_]]
    cd2Result.get._1 shouldBe 200
    cd2Result.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 500 on bad syntax" taggedAs (LocalTest) in {
    // TODO no it shouldn't (400 is better)
    // the statement below is bad because we don't know the protocol hdddddfs
    val badCdStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "hdddddfs:///jubatus-on-yarn/sample/shogun_data.json")"""
    val cdResult = sendJubaQL(badCdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class CreateModelSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE MODEL" should "return HTTP 200 on correct syntax" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200
    cmResult.get._2 \ "result" shouldBe JString("CREATE MODEL (started)")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if referenced feature functions do not exist" taggedAs (LocalTest, JubatusTest) in {
    val badCmStmt = """CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH hoge CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'"""
    val cmResult = sendJubaQL(badCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  // TODO ignored because server currently ignores the bad syntax
  it should "return HTTP 400 on bad syntax" taggedAs (LocalTest, JubatusTest) ignore {
    // the statement below is bad because "hello" is not a valid keyword
    val badCmStmt = """CREATE CLASSIFIER (hello: label) AS name WITH id CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'"""
    val cmResult = sendJubaQL(badCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  "CREATE MODEL(Production Mode)" should "return HTTP 200 on correct syntax and application-name of the as expected" taggedAs (JubatusTest) in {
    // override before() processing
    if (process != null) process.destroy()
    // start production-mode processor
    val startResult = startProcessor("-Drun.mode=production -Djubaql.zookeeper=127.0.0.1:2181 -Djubaql.checkpointdir=file:///tmp/spark -Djubaql.gateway.address=testAddress:1234 -Djubaql.processor.sessionId=1234567890abcdeABCDE")
    process = startResult._1
    stdout = startResult._2
    sendJubaQL = startResult._3

    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200
    cmResult.get._2 \ "result" shouldBe JString("CREATE MODEL (started)")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0

    // check application-name
    stdout.toString should include("starting JubatusOnYarn:testAddress:1234:1234567890abcdeABCDE:classifier:test1")
  }

  "CREATE MODEL(Production Mode) without SystemProperty" should "return HTTP 200 on correct syntax and application-name of the as expected" taggedAs (JubatusTest) in {

    // override before() processing
    if (process != null) process.destroy()
    // start production-mode processor (without System Property)
    val startResult = startProcessor("-Drun.mode=production -Djubaql.zookeeper=127.0.0.1:2181 -Djubaql.checkpointdir=file:///tmp/spark")
    process = startResult._1
    stdout = startResult._2
    sendJubaQL = startResult._3

    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200
    cmResult.get._2 \ "result" shouldBe JString("CREATE MODEL (started)")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0

    // check application-name
    stdout.toString should include("starting JubatusOnYarn:::classifier:test1")
  }
}

class CreateStreamFromSelectSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE STREAM" should "return HTTP 200 if a referenced data source exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 200 if a referenced created stream exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    val anotherGoodCsStmt = """CREATE STREAM ds3 FROM SELECT label FROM ds2"""
    val csResult2 = sendJubaQL(anotherGoodCsStmt)
    if (csResult2.get._1 != 200)
      println(stdout.toString)
    csResult2.get._1 shouldBe 200
    csResult2.get._2 \ "result" shouldBe JString("CREATE STREAM")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if the referenced stream does not exist" taggedAs (LocalTest) in {
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("unknown streams: ds1")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if the referenced streams come from different data sources" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt.replace("ds1", "dsA"))
    cdResult shouldBe a[Success[_]]
    val cd2Result = sendJubaQL(goodCdStmt.replace("ds1", "dsB"))
    cd2Result shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsStmt.replace("FROM ds1", "FROM dsA JOIN dsB"))
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("you cannot use streams from multiple different " +
      "data sources in one statement")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if referenced data source was already processed" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // start processing
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    // create stream
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("data source 'ds1' cannot accept further statements")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if a stream with that name already exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    val anotherGoodCsStmt = """CREATE STREAM ds2 FROM SELECT label FROM ds1"""
    val csResult2 = sendJubaQL(anotherGoodCsStmt)
    if (csResult2.get._1 != 400)
      println(stdout.toString)
    csResult2.get._1 shouldBe 400
    csResult2.get._2 \ "result" shouldBe JString("stream 'ds2' already exists")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "select correct values in explicitly declared columns and survive empty batches" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL("""CREATE STREAM test FROM SELECT label FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")
    val lsResult = sendJubaQL("LOG STREAM test")
    lsResult shouldBe a[Success[_]]
    lsResult.get._1 shouldBe 200
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)

    // for non-empty batches, select the value of the respective column
    val expectedFirstBatch = "STREAM: test\nlabel StringType\n徳川\n徳川"
    stdout.toString should include(expectedFirstBatch)

    // empty batches are not a problem if the schema is declared
    val expectedEmptyBatch = "STREAM: test\nlabel StringType\n\n"
    stdout.toString should include(expectedEmptyBatch)

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "select null for non-existing values in declared columns and survive empty batches" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL("""CREATE DATASOURCE ds1 (abc string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")""")
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL("""CREATE STREAM test FROM SELECT abc FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")
    val lsResult = sendJubaQL("LOG STREAM test")
    lsResult shouldBe a[Success[_]]
    lsResult.get._1 shouldBe 200
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")

    // for non-empty batches, select null
    waitUntilDone("ds1", 6000)
    val expected = "STREAM: test\nabc StringType\nnull\nnull"

    stdout.toString should include(expected)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "select correct values in inferred columns and FAIL at runtime on an empty batch" taggedAs (LocalTest) in {
    // TODO no it should not fail
    val cdResult = sendJubaQL("""CREATE DATASOURCE ds1 FROM (STORAGE: "file://src/test/resources/shogun_data.json")""")
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL("""CREATE STREAM test FROM SELECT label FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")
    val lsResult = sendJubaQL("LOG STREAM test")
    lsResult shouldBe a[Success[_]]
    lsResult.get._1 shouldBe 200
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)

    // for non-empty batches, select the value of the respective column
    val expectedFirstBatch = "STREAM: test\nlabel StringType\n徳川\n徳川"
    stdout.toString should include(expectedFirstBatch)

    // empty batches lead to failures
    stdout.toString should include("Unresolved attributes: 'label")
    stdout.toString should
      include("JobScheduler - Error generating jobs") // Spark logs error
    stdout.toString should
      include("HybridProcessor - Error while waiting for static processing end") // we log once
    stdout.toString should
      include("HybridProcessor - Error while setting up stream processing") // ... and again

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "FAIL at runtime when selecting values in non-declared columns" taggedAs (LocalTest) in {
    // TODO not it should not
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL("""CREATE STREAM test FROM SELECT nocolumn FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")
    val lsResult = sendJubaQL("LOG STREAM test")
    lsResult shouldBe a[Success[_]]
    lsResult.get._1 shouldBe 200
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)

    // for any batch, fail
    stdout.toString should include("Unresolved attributes: 'nocolumn")
    stdout.toString should
      include("JobScheduler - Error generating jobs") // Spark logs error
    stdout.toString should
      include("HybridProcessor - Error while waiting for static processing end") // we log once
    stdout.toString should
      include("HybridProcessor - Error while setting up stream processing") // ... and again

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class AggregatesInSlidingWindowSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  def checkCode(result: Try[(Int, JValue)], code: Int): Unit = {
    result shouldBe a[Success[_]]
    if (result.get._1 != code)
      println(stdout.toString)
    result.get._1 shouldBe code
    result.get._2 \ "result" match {
      case JString(msg) => info(msg)
      case other => info(result.get._2.toString)
    }
    // shut down
    sendJubaQL("SHUTDOWN") shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  val cdStmt = """CREATE DATASOURCE ds1 (gender string, age numeric, jubaql_timestamp string) FROM (STORAGE: "file://src/test/resources/dummydata/")"""

  "CREATE STREAM" should "return HTTP 400 if there is no column spec" taggedAs(LocalTest) in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 200 if there is exactly one column spec w/o alias" taggedAs(LocalTest) in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      "avg(age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  it should "return HTTP 200 if there is exactly one column spec w/alias" taggedAs(LocalTest) in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      "avg(age) AS hoge"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  it should "return HTTP 200 if there are multiple column specs" taggedAs(LocalTest) in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      "avg(age) AS hoge, histogram(age), concat('|', label) AS labels"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  ("avg" :: "stddev" :: "linapprox" :: "fourier" ::
    "wavelet" :: "maxelem" :: Nil).foreach(funcName => {
    funcName should "return HTTP 400 when used without any argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName()"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }

    it should "return HTTP 200 when used with exactly one argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName(age)"
      val result = sendJubaQL(stmt)
      checkCode(result, 200)
    }

    it should "return HTTP 400 when used with more than one argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName(age, label)"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }
  })

  ("quantile" :: "concat" :: Nil).foreach(funcName => {
    funcName should "return HTTP 400 when used without any argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName()"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }

    it should "return HTTP 200 when used with exactly one argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName(age)"
      val result = sendJubaQL(stmt)
      checkCode(result, 200)
    }

    it should "return HTTP 200 when used with two correct arguments" in {
      val param = if (funcName == "quantile") "0.5" else "'|'"
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName($param, age)"
      val result = sendJubaQL(stmt)
      checkCode(result, 200)
    }

    it should "return HTTP 400 when used with a non-foldable first argument" in {
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName(label, age)"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }

    it should "return HTTP 400 when used with an ill-typed first argument" in {
      val param = if (funcName == "concat") "0.5" else "'|'"
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName($param, age)"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }

    it should "return HTTP 400 when used with more than two arguments" in {
      val param = if (funcName == "quantile") "0.5" else "'|'"
      sendJubaQL(cdStmt) shouldBe a[Success[_]]
      val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
        "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
        s"$funcName($param, $param, label)"
      val result = sendJubaQL(stmt)
      checkCode(result, 400)
    }
  })

  "histogram" should "return HTTP 400 when used without any argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram()"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 200 when used with exactly one argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  // numBins

  it should "return HTTP 200 when used with a correct numBins argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(7, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  it should "return HTTP 400 when used with a non-foldable numBins argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(label, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with an ill-typed numBins argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram('x', age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with an invalid numBins argument" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(-1, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  // bounds

  it should "return HTTP 200 when used with correct bound arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(-1.0, 1.0, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  it should "return HTTP 400 when used with non-foldable bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(age, age, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with ill-typed bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram('x', 1.0, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with invalid bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(1.0, -1.0, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  // all arguments

  it should "return HTTP 200 when used with correct bound and numBins arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(-1.0, 1.0, 7, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 200)
  }

  it should "return HTTP 400 when used with non-foldable bound and bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(age, age, age, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with ill-typed bound and bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram('x', 1.0, 'y', age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  it should "return HTTP 400 when used with invalid bound and bounds arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(1.0, 1.0, 1, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }

  // higher number of arguments

  it should "return HTTP 400 when used with more than four arguments" in {
    sendJubaQL(cdStmt) shouldBe a[Success[_]]
    val stmt = "CREATE STREAM mystream FROM SLIDING WINDOW " +
      "(SIZE 2 ADVANCE 1 TUPLES) OVER ds1 WITH " +
      s"histogram(-1.0, 1.0, 7, 8, age)"
    val result = sendJubaQL(stmt)
    checkCode(result, 400)
  }
}

class CreateStreamFromSlidingWindowSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  val cdStmt = """CREATE DATASOURCE ds1 (gender string, age numeric, jubaql_timestamp string) FROM (STORAGE: "file://src/test/resources/dummydata/")"""

  "CREATE STREAM" should "return HTTP 200 if the referenced data source exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 2 ADVANCE 1 TUPLES) """ +
      """OVER ds1 WITH avg(age) AS avg_age"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if the referenced data source does not exist" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 2 ADVANCE 1 TUPLES) """ +
      """OVER nosuchds WITH avg(age) AS avg_age"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if the referenced function does not exist" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 2 ADVANCE 1 TUPLES) """ +
      """OVER ds1 WITH hello(age) AS avg_age"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "compute count-based sliding windows correctly" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 4 ADVANCE 1 TUPLES) """ +
      """OVER ds1 WITH avg(age) AS avg_age, maxelem(gender)"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    // create a view over the output (to make sure columns are selectable) and log it
    val cs2Result = sendJubaQL("CREATE STREAM ds3 FROM SELECT avg_age, maxelem FROM ds2")
    cs2Result shouldBe a[Success[_]]
    cs2Result.get._1 shouldBe 200
    val logResult = sendJubaQL("LOG STREAM ds3")
    logResult shouldBe a[Success[_]]
    logResult.get._1 shouldBe 200
    sendJubaQL("START PROCESSING ds1") shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    // values from the JSON files
    // (note that since OrderedFileInputDStream sorts by modification date,
    // make sure 1.json, 2.json, 3.json files are actually in that order)
    val ages = 21 :: 22 :: 23 :: 24 :: 21 :: 18 :: 22 :: 31 :: 23 :: Nil
    val genders = "mfffmfmfm".toList.map(_.toString)
    val correctValues = ages.zip(genders).sliding(4, 1).map(list => {
      val avgAge = list.map(_._1)
        .reduceLeft(_ + _).toDouble / list.size
      val mostFrequentGender = list.map(_._2)
        .groupBy(identity).maxBy(_._2.size)._1
      (avgAge, mostFrequentGender)
    }).take(3) // if we take 4, then the "maxelem" misbehavior will be visible
    val headerRow = "avg_age DoubleType | maxelem StringType"
    stdout.toString should include("STREAM: ds3\n" + headerRow)
    val outputString = correctValues.map(l => l._1 + " | " + l._2).mkString("\n")
    stdout.toString should include(headerRow + "\n" + outputString)
  }

  it should "compute count-based sliding windows with pre-condition correctly" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 4 ADVANCE 1 TUPLES) """ +
      """OVER ds1 WITH avg(age) AS avg_age, concat(gender) """ +
      """WHERE age > 21"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    // create a view over the output (to make sure columns are selectable) and log it
    val cs2Result = sendJubaQL("CREATE STREAM ds3 FROM SELECT avg_age, concat FROM ds2")
    cs2Result shouldBe a[Success[_]]
    cs2Result.get._1 shouldBe 200
    val logResult = sendJubaQL("LOG STREAM ds3")
    logResult shouldBe a[Success[_]]
    logResult.get._1 shouldBe 200
    sendJubaQL("START PROCESSING ds1") shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    // values from the JSON files
    // (note that since OrderedFileInputDStream sorts by modification date,
    // make sure 1.json, 2.json, 3.json files are actually in that order)
    val ages = 21 :: 22 :: 23 :: 24 :: 21 :: 18 :: 22 :: 31 :: 23 :: Nil
    val genders = "mfffmfmfm".toList.map(_.toString)
    val correctValues = ages.zip(genders).filter(_._1 > 21).sliding(4, 1).map(list => {
      val avgAge = list.map(_._1)
        .reduceLeft(_ + _).toDouble / list.size
      val genderConcat = list.map(_._2)
        .reduceLeft(_ + " " + _)
      (avgAge, genderConcat)
    }).take(3)
    val headerRow = "avg_age DoubleType | concat StringType"
    stdout.toString should include("STREAM: ds3\n" + headerRow)
    val outputString = correctValues.map(l => l._1 + " | " + l._2).mkString("\n")
    stdout.toString should include(headerRow + "\n" + outputString)
  }

  it should "compute count-based sliding windows with post-condition correctly" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM ds2 FROM SLIDING WINDOW (SIZE 4 ADVANCE 1 TUPLES) """ +
      """OVER ds1 WITH avg(age) AS avg_age, concat(gender) """ +
      """HAVING avg_age < 25"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    // create a view over the output (to make sure columns are selectable) and log it
    val cs2Result = sendJubaQL("CREATE STREAM ds3 FROM SELECT avg_age, concat FROM ds2")
    cs2Result shouldBe a[Success[_]]
    cs2Result.get._1 shouldBe 200
    val logResult = sendJubaQL("LOG STREAM ds3")
    logResult shouldBe a[Success[_]]
    logResult.get._1 shouldBe 200
    sendJubaQL("START PROCESSING ds1") shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    // values from the JSON files
    // (note that since OrderedFileInputDStream sorts by modification date,
    // make sure 1.json, 2.json, 3.json files are actually in that order)
    val ages = 21 :: 22 :: 23 :: 24 :: 21 :: 18 :: 22 :: 31 :: 23 :: Nil
    val genders = "mfffmfmfm".toList.map(_.toString)
    val correctValues = ages.zip(genders).sliding(4, 1).map(list => {
      val avgAge = list.map(_._1)
        .reduceLeft(_ + _).toDouble / list.size
      val genderConcat = list.map(_._2)
        .reduceLeft(_ + " " + _)
      (avgAge, genderConcat)
    }).filter(_._1 < 25).take(3)
    val headerRow = "avg_age DoubleType | concat StringType"
    stdout.toString should include("STREAM: ds3\n" + headerRow)
    val outputString = correctValues.map(l => l._1 + " | " + l._2).mkString("\n")
    stdout.toString should include(headerRow + "\n" + outputString)
  }
}


class LogStreamSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "LOG STREAM" should "return HTTP 200 if a referenced data source exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val lsResult = sendJubaQL(goodLsStmt)
    lsResult shouldBe a[Success[_]]
    if (lsResult.get._1 != 200)
      println(stdout.toString)
    lsResult.get._1 shouldBe 200
    lsResult.get._2 \ "result" shouldBe JString("LOG STREAM")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 200 if a referenced created stream exists" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsStmt)
    csResult shouldBe a[Success[_]]
    val lsStmt = """LOG STREAM ds2"""
    val lsResult = sendJubaQL(lsStmt)
    lsResult shouldBe a[Success[_]]
    if (lsResult.get._1 != 200)
      println(stdout.toString)
    lsResult.get._1 shouldBe 200
    lsResult.get._2 \ "result" shouldBe JString("LOG STREAM")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "print the contents of the referenced stream" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL("CREATE STREAM ds2 FROM SELECT name FROM ds1 WHERE label = '徳川'")
    csResult shouldBe a[Success[_]]
    val lsResult = sendJubaQL("LOG STREAM ds2")
    lsResult shouldBe a[Success[_]]
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)
    stdout.toString should include("家康\n秀忠")
    stdout.toString should not include("more items")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if the referenced stream does not exist" taggedAs (LocalTest) in {
    val lsResult = sendJubaQL(goodLsStmt)
    lsResult shouldBe a[Success[_]]
    if (lsResult.get._1 != 400)
      println(stdout.toString)
    lsResult.get._1 shouldBe 400
    lsResult.get._2 \ "result" shouldBe JString("source 'ds1' not found")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}


class CreateStreamFromAnalyzeSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE STREAM" should "return HTTP 200 if referenced data source and model exist" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsfaStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 200)
      println(stdout.toString)
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csfaStmt = """CREATE STREAM output FROM ANALYZE ds BY MODEL test USING classify AS newcol"""
    val csfaResult = sendJubaQL(csfaStmt)
    csfaResult shouldBe a[Success[_]]

    // executed before UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    // executed after UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    // before the first update:
    stdout.toString should include("徳川 | 家康 | List()")
    // after the first update:
    stdout.toString should include regex("徳川 \\| 家康 \\| .*徳川,0.93333333")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with ANOMALY" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/lof.json").getLines().mkString("")
    val cdStmt = s"""CREATE ANOMALY MODEL test AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csfaStmt = """CREATE STREAM output FROM ANALYZE ds BY MODEL test USING calc_score AS newcol"""
    val csfaResult = sendJubaQL(csfaStmt)
    csfaResult shouldBe a[Success[_]]

    // executed before UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING add FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    // executed after UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    // before the first update:
    stdout.toString should include("徳川 | 家康 | 1.0")
    // after the first update:
    stdout.toString should include("徳川 | 家康 | 0.9990")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_id" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: id) AS team WITH unigram, * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csfaStmt = """CREATE STREAM output FROM ANALYZE ds BY MODEL test USING complete_row_from_id AS newcol"""
    val csfaResult = sendJubaQL(csfaStmt)
    csfaResult shouldBe a[Success[_]]

    // executed before UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    // executed after UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    // before the first update:
    stdout.toString should include regex("長野久義 .+Map\\(\\),Map\\(\\)")
    // after the first update:
    stdout.toString should include regex("長野久義 .+Map\\(\\),Map\\(.*OPS -> 0.6804.*\\)")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_data" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: id) AS team WITH unigram, * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val aStmt2 = """CREATE STREAM output FROM ANALYZE ds BY MODEL test USING complete_row_from_datum AS newcol"""
    val aResult2 = sendJubaQL(aStmt2)
    aResult2 shouldBe a[Success[_]]

    // executed before UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    // executed after UPDATE
    sendJubaQL("LOG STREAM output") shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    // before the first update:
    stdout.toString should include regex("長野久義 .+Map\\(\\),Map\\(\\)")
    // after the first update:
    stdout.toString should include regex("長野久義 .+Map\\(\\),Map\\(.*OPS -> 0.6804.*\\)")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if referenced data source does not exist" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsfaStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("source 'ds1' not found")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if referenced data source was already processed" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // start processing
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    // create stream
    val csResult = sendJubaQL(goodCsfaStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("data source 'ds1' cannot accept further statements")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if there is already a stream with the same name" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // create stream
    val csResult = sendJubaQL(goodCsfaStmt)
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    // create same stream again
    val cs2Result = sendJubaQL(goodCsfaStmt)
    cs2Result shouldBe a[Success[_]]
    if (cs2Result.get._1 != 400)
      println(stdout.toString)
    cs2Result.get._1 shouldBe 400
    cs2Result.get._2 \ "result" shouldBe JString("stream 'ds3' already exists")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if referenced model does not exist" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val csResult = sendJubaQL(goodCsfaStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("no model called 'test1'")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 if learner method does not match" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val badCsfaStmt = """CREATE STREAM ds3 FROM ANALYZE ds1 BY MODEL test1 USING something AS newcol"""
    val csResult = sendJubaQL(badCsfaStmt)
    csResult shouldBe a[Success[_]]
    if (csResult.get._1 != 400)
      println(stdout.toString)
    csResult.get._1 shouldBe 400
    csResult.get._2 \ "result" shouldBe JString("'something' is not a valid method for Classifier")

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class UpdateModelSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "UPDATE MODEL" should "return HTTP 200 when model and datasource are present" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200
    umResult.get._2 \ "result" shouldBe JString("UPDATE MODEL")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 when model is missing" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 when data source is missing" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 when method name does not match" taggedAs (LocalTest, JubatusTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val badUmStmt = """UPDATE MODEL test1 USING foobar FROM ds1"""
    val umResult = sendJubaQL(badUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 400
    umResult.get._2 \ "result" shouldBe JString("'foobar' is not a valid method for Classifier")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "FAIL at runtime when referencing a non-existing column" taggedAs (LocalTest, JubatusTest) in {
    // TODO no it should not
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS doesnotexist WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    stdout.toString should include("column named 'doesnotexist' not found")
    stdout.toString should
      include("HybridProcessor - Error while waiting for static processing end") // we log once
    stdout.toString should
      include("HybridProcessor - Error while setting up stream processing") // ... and again

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "FAIL at runtime when referencing a non-existing column as label/id" taggedAs (LocalTest, JubatusTest) in {
    // TODO no it should not
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: doesnotexist) AS name WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    stdout.toString should include("the given schema Map(label -> (0,StringType), " +
      "name -> (1,StringType)) does not contain a column named 'doesnotexist'")
    stdout.toString should
      include("HybridProcessor - Error while waiting for static processing end") // we log once
    stdout.toString should
      include("HybridProcessor - Error while setting up stream processing") // ... and again

    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class StartProcessingSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "START PROCESSING" should "return 200 when UPDATE was received before" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    spResult.get._2 \ "result" shouldBe JString("START PROCESSING")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "be able to do subsequent processing of multiple data sources" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    // define data source with processing
    val cd1Result = sendJubaQL(goodCdStmt)
    cd1Result shouldBe a[Success[_]]
    val um1Result = sendJubaQL(goodUmStmt)
    um1Result shouldBe a[Success[_]]
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)
    val stopResult = sendJubaQL("STOP PROCESSING")
    stopResult shouldBe a[Success[_]]
    // define another datasource with processing
    val cd2Result = sendJubaQL(goodCdStmt.replace("ds1", "ab1"))
    cd2Result shouldBe a[Success[_]]
    val um2Result = sendJubaQL(goodUmStmt.replace("ds1", "ab1"))
    um2Result shouldBe a[Success[_]]
    // start updating
    val sp2Result = sendJubaQL("START PROCESSING ab1")
    sp2Result shouldBe a[Success[_]]
    sp2Result.get._1 shouldBe 200
    sp2Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ab1", 3000)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "be able to do subsequent processing of multiple data sources with interleaved definitions" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cd1Result = sendJubaQL(goodCdStmt)
    cd1Result shouldBe a[Success[_]]
    val cd2Result = sendJubaQL(goodCdStmt.replace("ds1", "ab1"))
    cd2Result shouldBe a[Success[_]]
    val um1Result = sendJubaQL(goodUmStmt)
    um1Result shouldBe a[Success[_]]
    val um2Result = sendJubaQL(goodUmStmt.replace("ds1", "ab1"))
    um2Result shouldBe a[Success[_]]
    // start updating
    val sp1Result = sendJubaQL("START PROCESSING ds1")
    sp1Result shouldBe a[Success[_]]
    sp1Result.get._1 shouldBe 200
    sp1Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)
    val stopResult = sendJubaQL("STOP PROCESSING")
    stopResult shouldBe a[Success[_]]
    // start updating
    val sp2Result = sendJubaQL("START PROCESSING ab1")
    sp2Result shouldBe a[Success[_]]
    sp2Result.get._1 shouldBe 200
    sp2Result.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ab1", 3000)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 when referencing a non-existing data source" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds217")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 400
    spResult.get._2 \ "result" shouldBe JString("unknown data source: ds217")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 when no UPDATE was received before" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 400
    spResult.get._2 \ "result" shouldBe JString("there are no processing instructions")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 when running on a data source currently being processed" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    spResult.get._2 \ "result" shouldBe JString("START PROCESSING")
    // start updating (again)
    val sp2Result = sendJubaQL("START PROCESSING ds1")
    sp2Result shouldBe a[Success[_]]
    sp2Result.get._1 shouldBe 400
    sp2Result.get._2 \ "result" shouldBe JString("cannot start processing a data source in state Running")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 when running on a data source already processed" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    spResult.get._2 \ "result" shouldBe JString("START PROCESSING")
    waitUntilDone("ds1", 6000)
    val stopResult = sendJubaQL("STOP PROCESSING")
    stopResult shouldBe a[Success[_]]
    stopResult.get._1 shouldBe 200
    // start updating (again)
    val sp2Result = sendJubaQL("START PROCESSING ds1")
    sp2Result shouldBe a[Success[_]]
    sp2Result.get._1 shouldBe 400
    sp2Result.get._2 \ "result" shouldBe JString("cannot start processing a data source in state Finished")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 when running on a data source while another is currently being processed" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cd1Result = sendJubaQL(goodCdStmt)
    cd1Result shouldBe a[Success[_]]
    val cd2Result = sendJubaQL(goodCdStmt.replace("ds1", "ab1"))
    cd2Result shouldBe a[Success[_]]
    val um1Result = sendJubaQL(goodUmStmt)
    um1Result shouldBe a[Success[_]]
    val um2Result = sendJubaQL(goodUmStmt.replace("ds1", "ab1"))
    um2Result shouldBe a[Success[_]]
    // start updating
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    spResult.get._2 \ "result" shouldBe JString("START PROCESSING")
    // start updating (again)
    val sp2Result = sendJubaQL("START PROCESSING ab1")
    sp2Result shouldBe a[Success[_]]
    sp2Result.get._1 shouldBe 400
    sp2Result.get._2 \ "result" shouldBe JString("there is already a running process, try to run STOP PROCESSING first")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class AnalyzeSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "ANALYZE" should "return with a meaningful result when START PROCESSING was run" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    // query
    waitUntilDone("ds1", 3000)
    val aResult = sendJubaQL(goodAStmt)
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "predictions" shouldBe a[JArray]
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return with a meaningful result after STOP PROCESSING" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    val startResult = sendJubaQL("START PROCESSING ds1")
    startResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 3000)
    val spResult = sendJubaQL("STOP PROCESSING")
    spResult shouldBe a[Success[_]]
    // query
    val aResult = sendJubaQL(goodAStmt)
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "predictions" shouldBe a[JArray]
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER on a created STREAM" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM streamds FROM SELECT label AS group, name AS alias FROM ds"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: group) AS alias WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM streamds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"alias": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with ANOMALY" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/lof.json").getLines().mkString("")
    val cdStmt = s"""CREATE ANOMALY MODEL test AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING add FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING calc_score"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "score" match {
      case JDouble(score) => {
        // the result of calc_score seems to differ slightly between
        // machines/OSes, therefore we do a proximity check instead
        // of equality comparison
        Math.abs(score - 1.0917037) should be < 0.0005
      }
      case _ =>
        fail("Failed to parse returned content as an anomaly result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with ANOMALY on a created STREAM" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/lof.json").getLines().mkString("")
    val cdStmt = s"""CREATE ANOMALY MODEL test AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    // NB. Apparently, when we rename the name column to something else
    // (like "xname"), then the result of the anomaly check will differ slightly.
    // Therefore, we create two streams and rename twice (back to the original name).

    val csStmtTmp = """CREATE STREAM streamds_tmp FROM SELECT name AS xname FROM ds"""
    val csResultTmp = sendJubaQL(csStmtTmp)
    csResultTmp shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM streamds FROM SELECT xname AS name FROM streamds_tmp"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING add FROM streamds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING calc_score"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "score" match {
      case JDouble(score) => {
        // the result of calc_score seems to differ slightly between
        // machines/OSes, therefore we do a proximity check instead
        // of equality comparison
        Math.abs(score - 1.0917037) should be < 0.0005
      }
      case _ =>
        fail("Failed to parse returned content as an anomaly result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_id" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: id) AS * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE 'スレッジ' BY MODEL test USING complete_row_from_id"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "num_values" match {
      case JObject(list) =>
        val vals = list.collect({
          case (s, JDouble(j)) => (s, j)
        }).toMap
        Math.abs(vals("長打率") - 0.3539453148841858) should be < 0.00001
        Math.abs(vals("試合数") - 104.234375) should be < 0.00001
        Math.abs(vals("打数") - 331.5546875) should be < 0.00001
      case _ =>
        fail("there was no 'num_values' key")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_id on a created STREAM" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: newid) AS * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM streamds FROM SELECT id AS newid, * FROM ds"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM streamds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE 'スレッジ' BY MODEL test USING complete_row_from_id"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "num_values" match {
      case JObject(list) =>
        val vals = list.collect({
          case (s, JDouble(j)) => (s, j)
        }).toMap
        Math.abs(vals("長打率") - 0.3539453148841858) should be < 0.00001
        Math.abs(vals("試合数") - 104.234375) should be < 0.00001
        Math.abs(vals("打数") - 331.5546875) should be < 0.00001
      case _ =>
        fail("there was no 'num_values' key")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_data" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: id) AS * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"team":"巨人","打率":0.209,"試合数":65.0,"打席":149.0,"打数":129.0,"安打":27.0,"本塁打":0.0,"打点":8.0,"盗塁":2.0,"四球":12.0,"死球":0.0,"三振":28.0,"犠打":6.0,"併殺打":5.0,"長打率":0.256,"出塁率":0.273,"OPS":0.529,"RC27":1.96,"XR27":2.07}' BY MODEL test USING complete_row_from_datum"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "num_values" match {
      case JObject(list) =>
        val vals = list.collect({
          case (s, JDouble(j)) => (s, j)
        }).toMap
        Math.abs(vals("長打率") - 0.33874213695526123) should be < 0.00001
        Math.abs(vals("試合数") - 100.953125) should be < 0.00001
        Math.abs(vals("打数") - 307.8046875) should be < 0.00001
      case _ =>
        fail("there was no 'num_values' key")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with RECOMMENDER/from_data on a created STREAM" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds FROM (STORAGE: "file://src/test/resources/npb_similar_player_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/npb_similar_player.json").getLines().mkString("")
    val cdStmt = s"""CREATE RECOMMENDER MODEL test (id: id) AS * WITH id CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csStmt = """CREATE STREAM streamds FROM SELECT team AS chiimu, * FROM ds"""
    val csResult = sendJubaQL(csStmt)
    csResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM streamds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)

    // analyze
    val aStmt = """ANALYZE '{"chiimu":"巨人","打率":0.209,"試合数":65.0,"打席":149.0,"打数":129.0,"安打":27.0,"本塁打":0.0,"打点":8.0,"盗塁":2.0,"四球":12.0,"死球":0.0,"三振":28.0,"犠打":6.0,"併殺打":5.0,"長打率":0.256,"出塁率":0.273,"OPS":0.529,"RC27":1.96,"XR27":2.07}' BY MODEL test USING complete_row_from_datum"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "num_values" match {
      case JObject(list) =>
        val vals = list.collect({
          case (s, JDouble(j)) => (s, j)
        }).toMap
        Math.abs(vals("長打率") - 0.33874213695526123) should be < 0.00001
        Math.abs(vals("試合数") - 100.953125) should be < 0.00001
        Math.abs(vals("打数") - 307.8046875) should be < 0.00001
      case _ =>
        fail("there was no 'num_values' key")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "still return HTTP 200 when START PROCESSING was not run" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // query
    val aResult = sendJubaQL(goodAStmt)
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 200
    aResult.get._2 \ "result" \ "predictions" shouldBe a[JArray]
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on bad syntax" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // the below statement is bad because it references a nonexisting algorithm
    val aResult = sendJubaQL( """ANALYZE '{"name": "慶喜"}' BY MODEL test1 USING aNonExistingAlgorithm""")
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 400
    aResult.get._2 \ "result" shouldBe JString("cannot use model 'test1' with method 'aNonExistingAlgorithm'")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 when there is no model" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val aResult = sendJubaQL(goodAStmt)
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 400
    aResult.get._2 \ "result" shouldBe JString("model 'test1' does not exist")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class StopProcessingSpec
  extends FlatSpec
  with Matchers
  with HasKafkaPath
  with ProcessorTestManager {

  "STOP PROCESSING" should "stop within a moderate time when data is processed" taggedAs (HDFSTest, JubatusTest) in {
    val cmResult = sendJubaQL("""CREATE CLASSIFIER MODEL test1 (label: movie_type) AS title WITH id, description WITH id CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""")
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL( s"""CREATE DATASOURCE ds1 (movie_type string, title string, description string) FROM (STORAGE: "hdfs:///user/fluentd/dummy", STREAM: "kafka://$kafkaPath/dummy/1")""")
    cdResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL( """UPDATE MODEL test1 USING train FROM ds1""")
    umResult shouldBe a[Success[_]]
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    Thread.sleep(5000)
    // stop processing
    val startTime = System.nanoTime()
    val stopResult = sendJubaQL("STOP PROCESSING")
    stopResult shouldBe a[Success[_]]
    stopResult.get._1 shouldBe 200
    stopResult.get._2 \ "result" shouldBe a[JString]
    (stopResult.get._2 \ "result").asInstanceOf[JString].values should
      startWith("STOP PROCESSING")
    val executionTime = (System.nanoTime() - startTime)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    executionTime.toDouble should be < 25e9 // less than 25 seconds
  }

  it should "return HTTP 400 if no processing is running" taggedAs (HDFSTest, JubatusTest) in {
    val cmResult = sendJubaQL("""CREATE CLASSIFIER MODEL test1 (label: movie_type) AS title WITH id, description WITH id CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""")
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL( s"""CREATE DATASOURCE ds1 (movie_type string, title string, description string) FROM (STORAGE: "hdfs:///user/fluentd/dummy", STREAM: "kafka://$kafkaPath/dummy/1")""")
    cdResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL( """UPDATE MODEL test1 USING train FROM ds1""")
    umResult shouldBe a[Success[_]]
    Thread.sleep(2000)
    // stop processing
    val stopResult = sendJubaQL("STOP PROCESSING")
    stopResult shouldBe a[Success[_]]
    stopResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class StatusSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {
  "STATUS" should "display empty lists when nothing exists" taggedAs (LocalTest) in {
    // send only STATUS
    val stResult = sendJubaQL("STATUS")
    stResult shouldBe a[Success[_]]
    stResult.get._1 shouldBe 200
    stResult.get._2 \ "result" shouldBe JString("STATUS")
    stResult.get._2 \ "sources" shouldBe a[JObject]
    stResult.get._2 \ "models" shouldBe a[JObject]
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "display model information" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200
    // check STATUS
    val stResult = sendJubaQL("STATUS")
    stResult shouldBe a[Success[_]]
    stResult.get._1 shouldBe 200
    stResult.get._2 \ "result" shouldBe JString("STATUS")
    (stResult.get._2 \ "models").extractOpt[Map[String, String]] match {
      case Some(models) =>
        models.keys should contain("test1")
        models("test1") shouldBe "OK"
      case None =>
        fail("'models' key did not contain a map")
    }
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "display source information" taggedAs (LocalTest) in {
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    // check STATUS
    val stResult = sendJubaQL("STATUS")
    stResult shouldBe a[Success[_]]
    stResult.get._1 shouldBe 200
    stResult.get._2 \ "result" shouldBe JString("STATUS")
    (stResult.get._2 \ "sources").extractOpt[Map[String, String]] match {
      case Some(sources) =>
        sources.keys should contain("ds1")
        sources("ds1") shouldBe "Initialized"
      case None =>
        fail("'sources' key did not contain a map")
    }
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "update source information" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // check STATUS
    val stResultInit = sendJubaQL("STATUS")
    stResultInit shouldBe a[Success[_]]
    stResultInit.get._1 shouldBe 200
    stResultInit.get._2 \ "sources" \ "ds1" shouldBe JString("Initialized")
    // start processing
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    // check STATUS
    val stResultRunning = sendJubaQL("STATUS")
    stResultRunning shouldBe a[Success[_]]
    stResultRunning.get._1 shouldBe 200
    stResultRunning.get._2 \ "sources" \ "ds1" shouldBe JString("Running")
    // wait a bit
    Thread.sleep(30000)
    val stResultDone = sendJubaQL("STATUS")
    stResultDone shouldBe a[Success[_]]
    stResultDone.get._1 shouldBe 200
    stResultDone.get._2 \ "sources" \ "ds1" shouldBe JString("Finished")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0

  }
}

class ShutdownSpec
  extends FlatSpec
  with Matchers
  with HasKafkaPath
  with ProcessorTestManager {

  "SHUTDOWN" should "stop the running instance" taggedAs (LocalTest) in {
    // send only SHUTDOWN
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200
    sdResult.get._2 \ "result" shouldBe a[JString]
    (sdResult.get._2 \ "result").asInstanceOf[JString].values should startWith("SHUTDOWN")
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    stdout.toString should include("shut down successfully")
  }

  it should "stop even after datasource was created" taggedAs (LocalTest) in {
    // set up a data source
    val cmResult = sendJubaQL(goodCdStmt)
    cmResult shouldBe a[Success[_]]
    // send only SHUTDOWN
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200
    sdResult.get._2 \ "result" shouldBe a[JString]
    (sdResult.get._2 \ "result").asInstanceOf[JString].values should startWith("SHUTDOWN")
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    stdout.toString should include("shut down successfully")
  }

  it should "stop even after model was created" taggedAs (LocalTest, JubatusTest) in {
    // set up a data source
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    // send only SHUTDOWN
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200
    sdResult.get._2 \ "result" shouldBe a[JString]
    (sdResult.get._2 \ "result").asInstanceOf[JString].values should startWith("SHUTDOWN")
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    stdout.toString should include("shut down successfully")
  }

  it should "stop within a moderate time even when data is processed" taggedAs (HDFSTest, JubatusTest) in {
    val cmResult = sendJubaQL("""CREATE CLASSIFIER MODEL test1 (label: movie_type) AS title WITH id, description WITH id CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""")
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL( s"""CREATE DATASOURCE ds1 (movie_type string, title string, description string) FROM (STORAGE: "hdfs:///user/fluentd/dummy", STREAM: "kafka://$kafkaPath/dummy/1")""")
    cdResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL( """UPDATE MODEL test1 USING train FROM ds1""")
    umResult shouldBe a[Success[_]]
    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    Thread.sleep(5000)
    // shut down
    val startTime = System.nanoTime()
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._2 \ "result" shouldBe a[JString]
    (sdResult.get._2 \ "result").asInstanceOf[JString].values should startWith("SHUTDOWN")
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 25e9 // less than 25 seconds
  }
}

class CreateFunctionSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE FUNCTION" should "return HTTP 200 on correct syntax" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id(arg string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on bad syntax" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL("""CREATE FUNCTION id(arg string)""")
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on apparently bad JavaScript syntax" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id(arg string) RETURNS string LANGUAGE JavaScript AS $$
        |/
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on empty parameter list" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id() RETURNS string LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on bad parameter type" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id(arg hello) RETURNS string LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on bad return type" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id(arg string) RETURNS hello LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on too long parameter list" taggedAs (LocalTest) in {
    val cfResult = sendJubaQL(
      """CREATE FUNCTION id(arg1 string, arg2 string, arg3 string, arg4 string, arg5 string, arg6 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a function which takes a string argument" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION addABC(arg string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg + "ABC";
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT addABC(label) AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川ABC", "足利ABC", "北条ABC")
        Math.abs(scores("徳川ABC") - 0.07692306488752365) should be < 0.00001
        scores("足利ABC") shouldBe 0.0
        scores("北条ABC") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a function which takes two string arguments" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION concat(arg1 string, arg2 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg1 + arg2;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT concat(label, "ABC") AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川ABC", "足利ABC", "北条ABC")
        Math.abs(scores("徳川ABC") - 0.07692306488752365) should be < 0.00001
        scores("足利ABC") shouldBe 0.0
        scores("北条ABC") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  // TODO: generate tests which take many arguments
  it should "make callable a function which takes three string arguments" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION concat3(arg1 string, arg2 string, arg3 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg1 + arg2 + arg3;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT concat3(label, "AB", "C") AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川ABC", "足利ABC", "北条ABC")
        Math.abs(scores("徳川ABC") - 0.07692306488752365) should be < 0.00001
        scores("足利ABC") shouldBe 0.0
        scores("北条ABC") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a function which takes four string arguments" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION concat4(arg1 string, arg2 string, arg3 string, arg4 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg1 + arg2 + arg3 + arg4;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT concat4(label, "A", "B", "C") AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川ABC", "足利ABC", "北条ABC")
        Math.abs(scores("徳川ABC") - 0.07692306488752365) should be < 0.00001
        scores("足利ABC") shouldBe 0.0
        scores("北条ABC") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a function which takes five string arguments" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION concat5(arg1 string, arg2 string, arg3 string, arg4 string, arg5 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg1 + arg2 + arg3 + arg4 + arg5;
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT concat5(label, "A", "B", "C", "D") AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川ABCD", "足利ABCD", "北条ABCD")
        Math.abs(scores("徳川ABCD") - 0.07692306488752365) should be < 0.00001
        scores("足利ABCD") shouldBe 0.0
        scores("北条ABCD") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a function which takes arguments of different type" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cfResult = sendJubaQL(
      """CREATE FUNCTION multiply(n numeric, s string) RETURNS string LANGUAGE JavaScript AS $$
        |return Array(n + 1).join(s);
        |$$
      """.stripMargin)
    cfResult shouldBe a[Success[_]]
    cfResult.get._1 shouldBe 200
    cfResult.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cfResult2 = sendJubaQL(
      """CREATE FUNCTION concat(arg1 string, arg2 string) RETURNS string LANGUAGE JavaScript AS $$
        |return arg1 + arg2;
        |$$
      """.stripMargin)
    cfResult2 shouldBe a[Success[_]]
    cfResult2.get._1 shouldBe 200
    cfResult2.get._2 \ "result" shouldBe JString("CREATE FUNCTION")

    val cmStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val csResult = sendJubaQL("""CREATE STREAM ds2 FROM SELECT concat(multiply(3, label), "ABC") AS label, name FROM ds1""")
    csResult shouldBe a[Success[_]]
    csResult.get._1 shouldBe 200
    csResult.get._2 \ "result" shouldBe JString("CREATE STREAM")

    val umStmt = """UPDATE MODEL test USING train FROM ds2"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds1")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds1", 6000)

    // analyze
    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // now check the result
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川徳川徳川ABC", "足利足利足利ABC", "北条北条北条ABC")
        Math.abs(scores("徳川徳川徳川ABC") - 0.07692306488752365) should be < 0.00001
        scores("足利足利足利ABC") shouldBe 0.0
        scores("北条北条北条ABC") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}

class CreateTriggerFunctionSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  "CREATE TRIGGER FUNCTION" should "return HTTP 200 on correct syntax" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg1 string, arg2 string, arg3 string) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return 400 on bad syntax" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL("""CREATE TRIGGER FUNCTION doNothing(arg string)""")
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on apparently bad JavaScript syntax" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg string) LANGUAGE JavaScript AS $$
        |/
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on empty parameter list" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing() LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on bad parameter type" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg hello) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 400 on too long parameter list" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg1 string, arg2 string, arg3 string, arg4 string, arg5 string, arg6 string) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "make callable a trigger function which takes a string argument" taggedAs(LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION printLine(arg string) LANGUAGE JavaScript AS $$
        |println(arg);
        |$$
      """.stripMargin)
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val cdStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE printLine(label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 200
    ctResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER")

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cmStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    val lines = stdout.toString.split('\n')
    lines.filter(_ == "徳川").length shouldBe 14
    lines.filter(_ == "足利").length shouldBe 15
    lines.filter(_ == "北条").length shouldBe 15

    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)

    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]

    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
  }

  it should "make callable a trigger function which takes two string arguments" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION printLine(arg1 string, arg2 string) LANGUAGE JavaScript AS $$
        |println(arg1 + arg2);
        |$$
      """.stripMargin)
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val cdStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE printLine(label, name)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 200
    ctResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER")

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cmStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    val lines = stdout.toString.split('\n')
    lines.filter(_.startsWith("徳川")).length shouldBe 14
    lines.filter(_.startsWith("足利")).length shouldBe 15
    lines.filter(_.startsWith("北条")).length shouldBe 15

    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)

    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]

    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
  }

  it should "make callable a trigger function which takes arguments of different type" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    // NOTE: use while instead of for i = i + 1 instead of ++i in JavaScript to manage current parser implementation.
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION printLines(n numeric, label string) LANGUAGE JavaScript AS $$
        |var i = 0;
        |while (i < n) {
        |  println(label);
        |  i = i + 1;
        |}
        |$$
      """.stripMargin)
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val cdStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE printLines(2, label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 200
    ctResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER")

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cmStmt = s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH unigram CONFIG '$config'"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]

    val spResult = sendJubaQL("START PROCESSING ds")
    spResult shouldBe a[Success[_]]
    waitUntilDone("ds", 6000)
    val lines = stdout.toString.split('\n')
    lines.filter(_.startsWith("徳川")).length shouldBe 14 * 2
    lines.filter(_.startsWith("足利")).length shouldBe 15 * 2
    lines.filter(_.startsWith("北条")).length shouldBe 15 * 2

    val aStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify"""
    val aResult = sendJubaQL(aStmt)

    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]

    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        // the order of entries differs per machine/OS, so we use this
        // slightly complicated way of checking equality
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0
      case None =>
        fail("Failed to parse returned content as a classifier result")
    }
  }
}

class CreateTriggerSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  // cannot use BeforeAndAfter.before in this class
  def createDatasource(): Unit = {
    val cdStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200
    cdResult.get._2 \ "result" shouldBe JString("CREATE DATASOURCE")
  }

  // This may be moved to ProcessorTestManager.
  def shutdown(): Unit = {
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    val exitValue = process.exitValue
    exitValue shouldBe 0
  }

  "CREATE TRIGGER" should "return HTTP 200 on correct syntax." taggedAs (LocalTest) in {
    createDatasource()

    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg string) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE doNothing(label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 200
    ctResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER")

    shutdown()
  }

  it should "return HTTP 400 on unknown function" taggedAs (LocalTest) in {
    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg string) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE doNothing(label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on unknown data source" taggedAs (LocalTest) in {
    createDatasource()

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE doNothing(label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on wrong number of parameters" taggedAs (LocalTest) in {
    createDatasource()

    val ctfResult = sendJubaQL(
      """CREATE TRIGGER FUNCTION doNothing(arg string) LANGUAGE JavaScript AS $$
        |
        |$$
      """.stripMargin)
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 200
    ctfResult.get._2 \ "result" shouldBe JString("CREATE TRIGGER FUNCTION")

    val ctResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE doNothing(name, label)""")
    ctResult shouldBe a[Success[_]]
    ctResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on bad syntax" taggedAs (LocalTest) in {
    createDatasource()

    // RW (not ROW)
    val ctfResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH RW EXECUTE someFunction(label)""")
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on passing a non-function expression" taggedAs (LocalTest) in {
    createDatasource()

    val ctfResult = sendJubaQL("""CREATE TRIGGER ON ds FOR EACH ROW EXECUTE 1 + someFunction(label)""")
    ctfResult shouldBe a[Success[_]]
    ctfResult.get._1 shouldBe 400

    shutdown()
  }
}

class CreateFeatureFunctionSpec
  extends FlatSpec
  with Matchers
  with ProcessorTestManager {

  // This may be moved to ProcessorTestManager.
  def shutdown(): Unit = {
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    val exitValue = process.exitValue
    exitValue shouldBe 0
  }

  "CREATE FEATURE FUNCTION" should "return HTTP 200 on correct syntax" taggedAs (LocalTest) in {
    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION abc(arg string) LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200
    cffResult.get._2 \ "result" shouldBe JString("CREATE FEATURE FUNCTION")

    shutdown()
  }

  it should "return HTTP 400 on bad syntax" taggedAs (LocalTest) in {
    val cffResult = sendJubaQL( """CREATE FEATURE FUNCTION abc(arg string)""")
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on apparently bad JavaScript syntax" taggedAs (LocalTest) in {
    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION abc(arg string) LANGUAGE JavaScript AS $$
        |\
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on empty parameter list" taggedAs (LocalTest) in {
    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION abc() LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 400

    shutdown()
  }

  it should "return HTTP 400 on bad parameter type" taggedAs (LocalTest) in {
    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION abc(arg hello) LANGUAGE JavaScript AS $$
        |return arg;
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 400

    shutdown()
  }

  it should "work correctly with CLASSIFIER when * is specified" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION f(name string) LANGUAGE JavaScript AS $$
        |if (name == "Yoshinobu") // test for ANALYZE
        |  name = "慶喜";
        |else { // test for UPDATE
        |  switch (name) {
        |  case "Ieyasu":
        |    name = "家康";
        |    break;
        |  case "Hidetada":
        |    name = "秀忠";
        |    break;
        |  }
        |}
        |return {"value": name};
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_alpha_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS * WITH f CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name": "Yoshinobu"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER when * with a prefix is specified" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION f(name string) LANGUAGE JavaScript AS $$
        |if (name == "Yoshinobu") // test for ANALYZE
        |  name = "慶喜";
        |else { // test for UPDATE
        |  switch (name) {
        |  case "Ieyasu":
        |    name = "家康";
        |    break;
        |  case "Hidetada":
        |    name = "秀忠";
        |    break;
        |  }
        |}
        |return {"value": name};
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_alpha_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS nam* WITH f CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name": "Yoshinobu"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER when * with a suffix is specified" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION f(name string) LANGUAGE JavaScript AS $$
        |if (name == "Yoshinobu") // test for ANALYZE
        |  name = "慶喜";
        |else { // test for UPDATE
        |  switch (name) {
        |  case "Ieyasu":
        |    name = "家康";
        |    break;
        |  case "Hidetada":
        |    name = "秀忠";
        |    break;
        |  }
        |}
        |return {"value": name};
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_alpha_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS *ame WITH f CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name": "Yoshinobu"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER using a named parameter" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION f(name string) LANGUAGE JavaScript AS $$
        |if (name == "Yoshinobu") // test for ANALYZE
        |  name = "慶喜";
        |else { // test for UPDATE
        |  switch (name) {
        |  case "Ieyasu":
        |    name = "家康";
        |    break;
        |  case "Hidetada":
        |    name = "秀忠";
        |    break;
        |  }
        |}
        |return {"value": name};
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_alpha_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS name WITH f CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name": "Yoshinobu"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER using named parameters" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cffResult = sendJubaQL(
      """CREATE FEATURE FUNCTION concatNames(name1 string, name2 string) LANGUAGE JavaScript AS $$
        |var name = name1 + name2;
        |return {"value": name};
        |$$
      """.stripMargin)
    cffResult shouldBe a[Success[_]]
    cffResult.get._1 shouldBe 200

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name1 string, name2 string) FROM (STORAGE: "file://src/test/resources/shogun_splitted_name_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS (name1, name2) WITH concatNames CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name1": "慶", "name2": "喜"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "work correctly with CLASSIFIER when WITH clause is not specified" taggedAs (LocalTest, JubatusTest) in {
    implicit val formats = DefaultFormats

    val cdResult = sendJubaQL("""CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")""")
    cdResult shouldBe a[Success[_]]
    cdResult.get._1 shouldBe 200

    val config = Source.fromFile("src/test/resources/shogun_full.json").getLines().mkString("")
    val cmResult = sendJubaQL(s"""CREATE CLASSIFIER MODEL test (label: label) AS name CONFIG '$config'""")
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 200

    val umResult = sendJubaQL("""UPDATE MODEL test USING train FROM ds""")
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 200

    val spResult = sendJubaQL("""START PROCESSING ds""")
    spResult shouldBe a[Success[_]]
    spResult.get._1 shouldBe 200
    waitUntilDone("ds", 6000)

    // analyze
    val aResult = sendJubaQL("""ANALYZE '{"name": "慶喜"}' BY MODEL test USING classify""")
    // shutdown
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    sdResult.get._1 shouldBe 200

    // check
    aResult shouldBe a[Success[_]]
    if (aResult.get._1 != 200)
      println(stdout.toString)
    aResult.get._1 shouldBe 200
    (aResult.get._2 \ "result").extractOpt[ClassifierResult] match {
      case Some(pred) =>
        val scores = pred.predictions.map(res => (res.label, res.score)).toMap
        scores.keys.toList should contain only("徳川", "足利", "北条")
        Math.abs(scores("徳川") - 0.07692306488752365) should be < 0.00001
        scores("足利") shouldBe 0.0
        scores("北条") shouldBe 0.0

      case None =>
        fail("")
    }

    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }
}
