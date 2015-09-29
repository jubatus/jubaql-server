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

import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, FlatSpec}
import org.scalatest.EitherValues._
import org.apache.spark.SparkContext
import org.apache.commons.io.FileExistsException
import us.jubat.jubaql_server.processor.json.{JubaQLResponse, StatementProcessed}
import us.jubat.yarn.common.LearningMachineType
import us.jubat.yarn.client.JubatusYarnApplication

import scala.util.Try


/* This test case tests only the state-independent (helper) functions of
 * JubaQLService (such as `parseJson()` or `extractDatum()`). It does
 * not test interaction with external components or anything that
 * requires state change.
 * (The reason being that we need to kill the JVM that is running
 * the JubaQLProcessor to exit cleanly.)
 */
class JubaQLServiceHelperSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
  private var sc: SparkContext = null
  private var service: JubaQLServiceTester = null
  private var proService: JubaQLServiceProductionTester = null

  val throwExceptionName = "throwExceptionName"

  // create a subclass to test the protected methods
  class JubaQLServiceTester(sc: SparkContext) extends JubaQLService(sc, RunMode.Development, "file:///tmp/spark") {
    override def parseJson(in: String): Either[(Int, String), JubaQLAST] =
      super.parseJson(in)

    override def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] =
      super.takeAction(ast)
  }

  // create a subclass to test the protected methods for Production Mode
  class JubaQLServiceProductionTester(sc: SparkContext, runMode: RunMode) extends JubaQLService(sc, runMode, "file:///tmp/spark") {
    override def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] =
      super.takeAction(ast)
  }

  // create a subclass to test the protected methods
  class LocalJubatusApplicationTester(name: String) extends LocalJubatusApplication(null, name, LearningMachineType.Classifier, "jubaclassifier") {
    override def saveModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
      name match {
        case throwExceptionName =>
          throw new FileExistsException("exception for test")

        case _ =>
          this
      }
    }

    override def loadModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
      name match {
        case throwExceptionName =>
          throw new FileExistsException("exception for test")

        case _ =>
          this
      }
    }
  }

  "parseJson()" should "be able to parse JSON" taggedAs (LocalTest) in {
    val query = """
      CREATE DATASOURCE test1 (column_type1 string, column_type2 numeric, column_type3 boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
                """.stripMargin.trim
    val json = """{"query": "%s"}""".format(query.replace("\"", "\\\""))
    val result = service.parseJson(json)
    result.right.value shouldBe a[CreateDatasource]
  }

  it should "be able to parse JSON with additional fields" taggedAs (LocalTest) in {
    val query = """
      CREATE DATASOURCE test1 (column_type1 string, column_type2 numeric, column_type3 boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
                """.stripMargin.trim
    val json = """{"session_id": "test", "query": "%s"}""".format(query.replace("\"", "\\\""))
    val result = service.parseJson(json)
    result.right.value shouldBe a[CreateDatasource]
  }

  it should "return an error if the JSON contains a bogus query" taggedAs (LocalTest) in {
    val json = """{"query": "test"}"""
    val result = service.parseJson(json)
    result.left.value._1 shouldBe 400
  }

  it should "return an error if the JSON contains a non-string query" taggedAs (LocalTest) in {
    val json = """{"query": 27}"""
    val result = service.parseJson(json)
    result.left.value._1 shouldBe 400
  }

  it should "return an error if the JSON contains no query" taggedAs (LocalTest) in {
    val json = """{"foo": "bar"}"""
    val result = service.parseJson(json)
    result.left.value._1 shouldBe 400
  }

  it should "return an error if the string is no JSON" taggedAs (LocalTest) in {
    val json = """hello"""
    val result = service.parseJson(json)
    result.left.value._1 shouldBe 400
  }

  // SaveModel test
  "takeAction():SaveModel" should "return an success for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    val sp = result.right.value.asInstanceOf[StatementProcessed]
    sp.result shouldBe "SAVE MODEL"
  }

  it should "return error of non model for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:///home/data/models", "test001")
    val juba = new LocalJubatusApplicationTester("test")

    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error invalid model path2 for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:/tmp/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of saveModel method for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester(throwExceptionName)

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  it should "return success for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    val sp = result.right.value.asInstanceOf[StatementProcessed]
    sp.result shouldBe "SAVE MODEL"
  }

  it should "return error of non model for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val juba = new LocalJubatusApplicationTester("test")

    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path2 for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:/home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of saveModel method for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester(throwExceptionName)

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  // LoadModel test
  "takeAction():LoadModel" should "return an success for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    val sp = result.right.value.asInstanceOf[StatementProcessed]
    sp.result shouldBe "LOAD MODEL"
  }

  it should "return error of non model for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:///home/data/models", "test001")
    val juba = new LocalJubatusApplicationTester("test")

    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error invalid model path2 for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:/tmp/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of loadModel method for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester(throwExceptionName)

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  it should "return success for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    val sp = result.right.value.asInstanceOf[StatementProcessed]
    sp.result shouldBe "LOAD MODEL"
  }

  it should "return error of non model for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val juba = new LocalJubatusApplicationTester("test")

    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path2 for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:/home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of loadModel method for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "")
    val juba = new LocalJubatusApplicationTester(throwExceptionName)

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  override protected def beforeAll(): Unit = {
    sc = new SparkContext("local[3]", "JubaQL Processor Test")
    service = new JubaQLServiceTester(sc)

    val hosts: List[(String, Int)] = List(("localhost", 1111))
    proService = new JubaQLServiceProductionTester(sc, RunMode.Production(hosts))
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
