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

import java.io.{File, FileWriter, IOException}

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.SparkContext
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest._
import us.jubat.common.Datum
import us.jubat.jubaql_server.processor.json.JubaQLResponse

import scala.collection.JavaConversions._
import scala.concurrent.{Await => ScAwait}
import scala.sys.process._

class JubaQLExtractorSpec extends FlatSpec
with ShouldMatchers with EitherValues with BeforeAndAfterAll {
  private var sc: SparkContext = null
  private var service: JubaQLServiceTester = null

  "* WITH id" should "convert data with num and str" taggedAs(JubatusTest, LocalTest) in {
    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 27, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 3
    fvLines should contain("/foo$bar bar@str#tf/bin: 1")
    fvLines should contain("/f@num: 1.23")
    fvLines should contain("/hoge@num: 27")
  }

  "hoge WITH func" should "fail if func is unknown" taggedAs(JubatusTest, LocalTest) in {
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH func CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    // when submitted via service.processStmt this will already fail earlier
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 27, "f": 1.23}"""
    val thrown = the[RuntimeException] thrownBy extractDatum(cm, inputData)
    thrown.getMessage should startWith("feature function 'func' is not found")
  }

  "hoge WITH square_root" should "extract only hoge and use the square_root feature function on it" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS hoge WITH square_root CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 1
    jubatusInputDatum.getStringValues.size shouldBe 0
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 1
    fvLines should contain("/square_root#hoge@num: 4")
  }

  "foo WITH unigram" should "extract only foo and pass the value on to Jubatus" taggedAs(JubatusTest, LocalTest) in {
    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS foo WITH unigram CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 0
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 4
    fvLines should contain("/foo-unigram-jubaconv$ @unigram#tf/bin: 1")
    fvLines should contain("/foo-unigram-jubaconv$b@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$a@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$r@unigram#tf/bin: 2")
  }

  "foo WITH unigram, bar WITH bigram" should "extract only foo and bar and pass the values on to Jubatus" taggedAs(JubatusTest, LocalTest) in {
    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS foo WITH unigram, bar WITH bigram CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "bar": "hello", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 0
    jubatusInputDatum.getStringValues.size shouldBe 2
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 8
    fvLines should contain("/foo-unigram-jubaconv$ @unigram#tf/bin: 1")
    fvLines should contain("/foo-unigram-jubaconv$b@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$a@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$r@unigram#tf/bin: 2")
    fvLines should contain("/bar-bigram-jubaconv$he@bigram#tf/bin: 1")
    fvLines should contain("/bar-bigram-jubaconv$el@bigram#tf/bin: 1")
    fvLines should contain("/bar-bigram-jubaconv$ll@bigram#tf/bin: 1")
    fvLines should contain("/bar-bigram-jubaconv$lo@bigram#tf/bin: 1")
  }

  "hoge WITH square_root, foo WITH unigram, *" should "extract all values with the correct functions" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS hoge WITH square_root, foo WITH unigram, * CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 6
    fvLines should contain("/square_root#hoge@num: 4")
    fvLines should contain("/foo-unigram-jubaconv$ @unigram#tf/bin: 1")
    fvLines should contain("/foo-unigram-jubaconv$b@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$a@unigram#tf/bin: 2")
    fvLines should contain("/foo-unigram-jubaconv$r@unigram#tf/bin: 2")
    fvLines should contain("/f@num: 1.23")
  }

  "hoge WITH square_root, hoge" should "extract hoge twice, with square_root and with id" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS hoge WITH square_root, hoge CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 0
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 2
    fvLines should contain("/square_root#hoge@num: 4")
    fvLines should contain("/hoge@num: 16")
  }

  "(hoge, foo) WITH repeat" should "apply repeat to hoge and foo" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION repeat(n numeric, s string) LANGUAGE JavaScript AS $$
        |return {"value": Array(n+1).join(s)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS (hoge, foo) WITH repeat CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 2, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 0
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 1
    fvLines should contain("/repeat#hoge,foo$bar barbar bar@str#tf/bin: 1")
  }

  "(hoge, foo) WITH repeat, hoge WITH square_root, *" should "apply repeat to hoge and foo, square_root to hoge, id to the rest" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION repeat(n numeric, s string) LANGUAGE JavaScript AS $$
        |return {"value": Array(n+1).join(s)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    val cffStmt2 =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt2) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS (hoge, foo) WITH repeat, hoge WITH square_root, * CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 4, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 3
    fvLines should contain("/repeat#hoge,foo$bar barbar barbar barbar bar@str#tf/bin: 1")
    fvLines should contain("/square_root#hoge@num: 2")
    fvLines should contain("/f@num: 1.23")
  }

  "hoge WITH stats" should "apply the multi-valued stats function to hoge" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION stats(n numeric) LANGUAGE JavaScript AS $$
        |return {"plus": n, "minus": -n};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS hoge WITH stats CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 4, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 0
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 2
    fvLines should contain("/stats#hoge#plus@num: 4")
    fvLines should contain("/stats#hoge#minus@num: -4")
  }

  "h*" should "pick only columns starting with h" taggedAs(JubatusTest, LocalTest) in {
    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS h* CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hello": "test", "hoge": 27, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 1
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 2
    fvLines should contain("/hello$test@str#tf/bin: 1")
    fvLines should contain("/hoge@num: 27")
  }

  "*e" should "pick only columns ending with e" taggedAs(JubatusTest, LocalTest) in {
    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS *e CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foe": "bar bar", "hello": "test", "hoge": 27, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 1
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 2
    fvLines should contain("/foe$bar bar@str#tf/bin: 1")
    fvLines should contain("/hoge@num: 27")
  }

  "h* WITH square_root, *" should "apply square_root on columns starting with h and the rest with id" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS h* WITH square_root, * CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foo": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 2
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 3
    fvLines should contain("/square_root#hoge@num: 4")
    fvLines should contain("/foo$bar bar@str#tf/bin: 1")
    fvLines should contain("/f@num: 1.23")
  }

  "h* WITH square_root, *e" should "pick hoge only with square_root" taggedAs(JubatusTest, LocalTest) in {
    // create a feature function (this changes state of service)
    val cffStmt =
      """
        |CREATE FEATURE FUNCTION square_root(x numeric) LANGUAGE JavaScript AS $$
        |return {"value": Math.sqrt(x)};
        |$$
      """.stripMargin
    service.processStmt(cffStmt) shouldBe a[Right[_, _]]

    // CREATE MODEL statement
    val cmStmt =
      """CREATE CLASSIFIER MODEL test1 (label: label) AS h* WITH square_root, *e CONFIG
        |'{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'""".stripMargin
    val parsedStmt = service.parseStmt(cmStmt).right.get
    parsedStmt shouldBe a[CreateModel]
    val cm = parsedStmt.asInstanceOf[CreateModel]
    // extract datum from input data
    val inputData = """{"label": "abc", "foe": "bar bar", "hoge": 16, "f": 1.23}"""
    val jubatusInputDatum = extractDatum(cm, inputData)
    jubatusInputDatum.getNumValues.size shouldBe 1
    jubatusInputDatum.getStringValues.size shouldBe 1
    // convert datum to feature vector
    val jsonConfig = service.jubatusJsonConfig(cm).right.get
    val fvLines = convertToFeatureVector(jsonConfig, jubatusInputDatum)
    fvLines.size shouldBe 2
    fvLines should contain("/square_root#hoge@num: 4")
    fvLines should contain("/foe$bar bar@str#tf/bin: 1")
  }

  // create a subclass to access the protected methods
  class JubaQLServiceTester(sc: SparkContext) extends JubaQLService(sc, RunMode.Development, "file:///tmp/spark") {
    // parses a JubaQL statement and returns the corresponding AST subclass
    // on success
    def parseStmt(s: String): Either[(Int, String), JubaQLAST] = {
      logger.debug("request body: %s".format(s))
      val command = Some(compact(render(JObject("query" -> JString(s)))))
      val parsedCommand = command.map(parseJson)
      parsedCommand.getOrElse(Left((500, "error")))
    }

    // parses and executes a JubaQLStatement and returns the corresponding
    // AST subclass and a response object on success
    def processStmt(s: String): Either[(Int, String), (JubaQLAST, JubaQLResponse)] = {
      val parsedCommand = Some(parseStmt(s))
      val actionResult = parsedCommand.map(_.right.flatMap(cmd => {
        takeAction(cmd).right.map((cmd, _))
      }))
      actionResult.getOrElse(Left((500, "error")))
    }

    // this creates a JSON configuration string for Jubatus as
    // computed from a CREATE MODEL statement
    def jubatusJsonConfig(cm: CreateModel): Either[(Int, String), String] = {
      val jsonString = cm.jubatusConfigJsonOrPath match {
        case Left(jsonString) => jsonString
        case Right(file) => readConfigFile(file)
      }
      complementInputJson(jsonString).right.map(j => pretty(render(j)))
    }

    // returns the logger used by the JubaQLService
    def myLogger(): Logger = {
      logger
    }
  }

  // extract a datum from the given JSON-shaped data using the rules
  // from the given CreateModel statement (will use the service's current
  // state)
  def extractDatum(cm: CreateModel, data: String): Datum = {
    DatumExtractor.extract(cm, data, service.featureFunctions, service.myLogger)
  }

  // convert the given datum to a feature vector using the given
  // Jubatus configuration. The output are the lines output by
  // `jubaconv -i json -o fv`
  def convertToFeatureVector(jsonConfig: String, datum: Datum): List[String] = {
    def writeStringToFile(file: File, data: String, appending: Boolean = false) =
      using(new FileWriter(file, appending))(_.write(data))

    def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
      try f(resource) finally resource.close()

    try {
      // write JSON config to temp file
      val confFile = File.createTempFile("jubaql_", ".json")
      using(new FileWriter(confFile))(writer => writer.write(jsonConfig))
      scala.compat.Platform.collectGarbage() // JVM Windows related bug workaround JDK-4715154
      confFile.deleteOnExit()

      // write input data to temp file
      val inputFile = File.createTempFile("jubaql_input_", ".json")
      using(new FileWriter(inputFile))(writer => writer.write(datumToJson(datum)))
      scala.compat.Platform.collectGarbage() // JVM Windows related bug workaround JDK-4715154
      inputFile.deleteOnExit()

      val convOutput = ("jubaconv" :: "-i" :: "json" :: "-o" :: "fv" :: "-c" ::
        confFile.toString :: Nil).#<(inputFile).!!
      convOutput.trim.split('\n').toList
    } catch {
      case e: IOException =>
        Nil
    }
  }

  def datumToJson(d: Datum): String = {
    val nvals = JObject(d.getNumValues.toList.map(kv => kv.key -> JDouble(kv.value)): _*)
    val svals = JObject(d.getStringValues.toList.map(kv => kv.key -> JString(kv.value)): _*)
    pretty(render(nvals ~ svals))
  }

  override protected def beforeAll(): Unit = {
    sc = new SparkContext("local[3]", "JubaQL Processor Test")
    service = new JubaQLServiceTester(sc)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
