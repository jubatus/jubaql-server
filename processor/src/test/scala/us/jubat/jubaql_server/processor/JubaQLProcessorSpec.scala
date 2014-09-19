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
import scala.io.Source
import org.scalatest._
import java.nio.file.{Paths, Files}
import dispatch._
import dispatch.Defaults._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.util.{Success, Failure, Try}
import us.jubat.jubaql_server.processor.json.ClassifierResult

/** Tests the correct behavior as viewed from the outside.
  */
class JubaQLProcessorSpec
  extends FlatSpec
  with Matchers
  with HasKafkaPath
  with BeforeAndAfter
  with BeforeAndAfterAll {

  implicit val formats = DefaultFormats

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
    process.destroy()
  }

  val goodCdStmt = """CREATE DATASOURCE ds1 (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
  val goodCmStmt = """CREATE CLASSIFIER MODEL test1 WITH (label: "label", datum: "name") config = '{"method": "AROW","converter": {  "num_filter_types": {},  "num_filter_rules": [],  "string_filter_types": {},   "string_filter_rules": [],    "num_types": {},  "num_rules": [],"string_types": {"unigram": { "method": "ngram", "char_num": "1" }},"string_rules": [{ "key": "*", "type": "unigram", "sample_weight": "bin", "global_weight": "bin" } ]},"parameter": {"regularization_weight" : 1.0}}'"""
  val goodUmStmt = """UPDATE MODEL test1 USING train FROM ds1"""
  val goodAStmt = """ANALYZE '{"name": "慶喜"}' BY MODEL test1 USING classify"""


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

  // TODO ignored because server currently ignores the bad syntax
  it should "return HTTP 500 on bad syntax" taggedAs (LocalTest, JubatusTest) ignore {
    // TODO no it shouldn't (400 is better)
    // the statement below is bad because "hello" is not a valid keyword
    val badCmStmt = """CREATE CLASSIFIER MODEL test1 WITH(hello: "label", datum: "name") config = '{"method": "AROW","converter": {  "num_filter_types": {},  "num_filter_rules": [],  "string_filter_types": {},   "string_filter_rules": [],    "num_types": {},  "num_rules": [],"string_types": {"unigram": { "method": "ngram", "char_num": "1" }},"string_rules": [{ "key": "*", "type": "unigram", "sample_weight": "bin", "global_weight": "bin" } ]},"parameter": {"regularization_weight" : 1.0}}'"""
    val cmResult = sendJubaQL(badCmStmt)
    cmResult shouldBe a[Success[_]]
    cmResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }


  "UPDATE MODEL" should "return HTTP 200 when model and datasource are present" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // start updating
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

  it should "return HTTP 500 when model is missing" taggedAs (LocalTest) in {
    // TODO no it shouldn't (400 is better)
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 500 when data source is missing" taggedAs (LocalTest, JubatusTest) in {
    // TODO no it shouldn't (400 is better)
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    umResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }


  "ANALYZE" should "return with a meaningful result when UPDATE was run" taggedAs (LocalTest, JubatusTest) in {
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val umResult = sendJubaQL(goodUmStmt)
    umResult shouldBe a[Success[_]]
    // query
    Thread.sleep(1000)
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
    Thread.sleep(1000)
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
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/shogun.json").getLines().mkString("")
    val cdStmt = s"""CREATE CLASSIFIER MODEL test WITH (label: "label", datum: "name") config = '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING train FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]
    Thread.sleep(2500)

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

  it should "work correctly with ANOMALY" taggedAs (LocalTest, JubatusTest) in {
    val cmStmt = """CREATE DATASOURCE ds (label string, name string) FROM (STORAGE: "file://src/test/resources/shogun_data.json")"""
    val cmResult = sendJubaQL(cmStmt)
    cmResult shouldBe a[Success[_]]

    val config = Source.fromFile("src/test/resources/lof.json").getLines().mkString("")
    val cdStmt = s"""CREATE ANOMALY MODEL test WITH (label: "label", datum: "name") config = '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING add FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]
    Thread.sleep(2500)

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
        Math.abs(score - 1.006646) should be < 0.0005
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
    val cdStmt = s"""CREATE RECOMMENDER MODEL test WITH (id: "id", datum: ["team", "打率", "試合数", "打席", "打数", "安打", "本塁打", "打点", "盗塁", "四球", "死球", "三振", "犠打", "併殺打", "長打率", "出塁率", "OPS", "RC27", "XR27"]) config = '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]
    Thread.sleep(2500)

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
    val cdStmt = s"""CREATE RECOMMENDER MODEL test WITH (id: "id", datum: ["team", "打率", "試合数", "打席", "打数", "安打", "本塁打", "打点", "盗塁", "四球", "死球", "三振", "犠打", "併殺打", "長打率", "出塁率", "OPS", "RC27", "XR27"]) config = '$config'"""
    val cdResult = sendJubaQL(cdStmt)
    cdResult shouldBe a[Success[_]]

    val umStmt = """UPDATE MODEL test USING update_row FROM ds"""
    val umResult = sendJubaQL(umStmt)
    umResult shouldBe a[Success[_]]
    Thread.sleep(2500)

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

  it should "still return HTTP 200 when UPDATE was not run" taggedAs (LocalTest, JubatusTest) in {
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

  it should "return HTTP 500 on bad syntax" taggedAs (LocalTest, JubatusTest) in {
    // TODO no it shouldn't (400 is better)
    val cmResult = sendJubaQL(goodCmStmt)
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    // the below statement is bad because it references a nonexisting algorithm
    val aResult = sendJubaQL( """ANALYZE '{"name": "慶喜"}' BY MODEL test1 USING aNonExistingAlgorithm""")
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }

  it should "return HTTP 500 when there is no model" taggedAs (LocalTest) in {
    // TODO no it shouldn't (400 is better)
    val cdResult = sendJubaQL(goodCdStmt)
    cdResult shouldBe a[Success[_]]
    val aResult = sendJubaQL(goodAStmt)
    aResult shouldBe a[Success[_]]
    aResult.get._1 shouldBe 500
    // shut down
    val sdResult = sendJubaQL("SHUTDOWN")
    sdResult shouldBe a[Success[_]]
    // wait until shutdown
    val exitValue = process.exitValue()
    exitValue shouldBe 0
  }


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
    val cmResult = sendJubaQL( """CREATE CLASSIFIER MODEL test1 WITH(label: "movie_type", datum: ["title", "description"]) config = '{"method": "AROW","converter": {  "num_filter_types": {},  "num_filter_rules": [],  "string_filter_types": {},   "string_filter_rules": [],    "num_types": {},  "num_rules": [],"string_types": {"unigram": { "method": "ngram", "char_num": "1" }},"string_rules": [{ "key": "*", "type": "unigram", "sample_weight": "bin", "global_weight": "bin" } ]},"parameter": {"regularization_weight" : 1.0}}'""")
    cmResult shouldBe a[Success[_]]
    val cdResult = sendJubaQL( s"""CREATE DATASOURCE ds1 (movie_type string, title string, description string) FROM (STORAGE: "hdfs:///user/fluentd/dummy", STREAM: "kafka://$kafkaPath/dummy/1")""")
    cdResult shouldBe a[Success[_]]
    // start updating
    val umResult = sendJubaQL( """UPDATE MODEL test1 USING train FROM ds1""")
    umResult shouldBe a[Success[_]]
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


  override protected def beforeAll(): Unit = {
    // if there is no script to start the application yet, generate it
    if (!Files.exists(Paths.get("start-script/run"))) {
      Seq("sbt", "start-script").!
    }
    super.beforeAll()
  }

  protected def startProcessor(): (Process,
    StringBuffer, String => Try[(Int, JValue)]) = {
    val command = Seq("./start-script/run")
    val (logger, stdoutBuffer, stderrBuffer) = getProcessLogger()
    val process = command run logger
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
