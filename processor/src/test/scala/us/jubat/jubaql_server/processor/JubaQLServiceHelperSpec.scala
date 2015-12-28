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
import org.scalatest.PrivateMethodTester._
import org.apache.spark.SparkContext
import org.apache.commons.io.FileExistsException
import us.jubat.jubaql_server.processor.json.{JubaQLResponse, StatementProcessed, StatusResponse}
import us.jubat.yarn.common.LearningMachineType
import us.jubat.yarn.client.{JubatusYarnApplication, Resource, JubatusYarnApplicationStatus}
import scala.collection.mutable.LinkedHashMap
import scala.collection.Map

import scala.util.Try


/* This test case tests only the state-independent (helper) functions of
 * JubaQLService (such as `parseJson()` or `extractDatum()`). It does
 * not test interaction with external components or anything that
 * requires state change.
 * (The reason being that we need to kill the JVM that is running
 * the JubaQLProcessor to exit cleanly.)
 */
object JubaQLServiceHelperSpec {
  val throwExceptionName = "throwExceptionName"
}
class JubaQLServiceHelperSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
  private var sc: SparkContext = null
  private var service: JubaQLServiceTester = null
  private var proService: JubaQLServiceProductionTester = null

  // create a subclass to test the protected methods
  class JubaQLServiceTester(sc: SparkContext) extends JubaQLService(sc, RunMode.Development, "file:///tmp/spark") {
    override def parseJson(in: String): Either[(Int, String), JubaQLAST] =
      super.parseJson(in)

    override def complementResource(resourceJsonString: Option[String]): Either[(Int, String), Resource] =
      super.complementResource(resourceJsonString)

    override def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] =
      super.takeAction(ast)

      override def getSourcesStatus(): Map[String, Any] =
      super.getSourcesStatus()

      override def getModelsStatus(): Map[String, Any] =
      super.getModelsStatus()

      override def getProcessorStatus(): Map[String, Any] =
      super.getProcessorStatus()
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
        case JubaQLServiceHelperSpec.throwExceptionName =>
          throw new FileExistsException("exception for test")

        case _ =>
          this
      }
    }

    override def loadModel(aModelPathPrefix: org.apache.hadoop.fs.Path, aModelId: String): Try[JubatusYarnApplication] = Try {
      name match {
        case JubaQLServiceHelperSpec.throwExceptionName =>
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
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

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
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

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
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

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
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  "complementResource()" should "success recource config" taggedAs (LocalTest) in {
    // 指定なし
    var result = service.complementResource(None)
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe 128
        value.proxyMemory shouldBe 32
        value.masterCores shouldBe 1
        value.priority shouldBe 0
        value.containerMemory shouldBe 128
        value.memory shouldBe 256
        value.virtualCores shouldBe 1
        value.containerNodes shouldBe null
        value.containerRacks shouldBe null

      case _ =>
        fail()
    }
    // 必要なキーなし
    var resConfig = """{"test1": 256, "test2": 128}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe 128
        value.proxyMemory shouldBe 32
        value.masterCores shouldBe 1
        value.priority shouldBe 0
        value.containerMemory shouldBe 128
        value.memory shouldBe 256
        value.virtualCores shouldBe 1
        value.containerNodes shouldBe null
        value.containerRacks shouldBe null

      case _ =>
        fail()
    }
    // 必要なキーなし
    resConfig = """{}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe 128
        value.proxyMemory shouldBe 32
        value.masterCores shouldBe 1
        value.priority shouldBe 0
        value.containerMemory shouldBe 128
        value.memory shouldBe 256
        value.virtualCores shouldBe 1
        value.containerNodes shouldBe null
        value.containerRacks shouldBe null

      case _ =>
        fail()
    }
  }

  it should "error recource config for invalid format" taggedAs (LocalTest) in {
    var resConfig = """""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    resConfig = """{"applicationmaster_memory", "jubatus_proxy_memory"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    resConfig = """{applicationmaster_memory: 256, jubatus_proxy_memory: 128}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    resConfig = """{"applicationmaster_memory":256, 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for applicationmaster_memory" taggedAs (LocalTest) in {
    var resConfig = """{"applicationmaster_memory": 256}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe 256

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"applicationmaster_memory": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"applicationmaster_memory": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterMemory shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for applicationmaster_memory" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"applicationmaster_memory": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"applicationmaster_memory": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"applicationmaster_memory": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for jubatus_proxy_memory" taggedAs (LocalTest) in {
    var resConfig = """{"jubatus_proxy_memory": 16}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.proxyMemory shouldBe 16

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"jubatus_proxy_memory": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.proxyMemory shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"jubatus_proxy_memory": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.proxyMemory shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for jubatus_proxy_memory" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"jubatus_proxy_memory": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"jubatus_proxy_memory": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"jubatus_proxy_memory": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for applicationmaster_cores" taggedAs (LocalTest) in {
    var resConfig = """{"applicationmaster_cores": 2}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterCores shouldBe 2

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"applicationmaster_cores": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterCores shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"applicationmaster_cores": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.masterCores shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for applicationmaster_cores" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"applicationmaster_cores": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"applicationmaster_cores": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"applicationmaster_cores": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for container_priority" taggedAs (LocalTest) in {
    var resConfig = """{"container_priority": 2}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.priority shouldBe 2

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"container_priority": 0}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.priority shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"container_priority": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.priority shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_priority" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"container_priority": -1}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"container_priority": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"container_priority": "1"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for container_memory" taggedAs (LocalTest) in {
    var resConfig = """{"container_memory": 256}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerMemory shouldBe 256

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"container_memory": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerMemory shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"container_memory": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerMemory shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_memory" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"container_memory": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"container_memory": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"container_memory": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for jubatus_server_memory" taggedAs (LocalTest) in {
    var resConfig = """{"jubatus_server_memory": 512}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.memory shouldBe 512

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"jubatus_server_memory": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.memory shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"jubatus_server_memory": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.memory shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for jubatus_server_memory" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"jubatus_server_memory": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"jubatus_server_memory": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"jubatus_server_memory": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for container_cores" taggedAs (LocalTest) in {
    var resConfig = """{"container_cores": 2}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.virtualCores shouldBe 2

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"container_cores": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.virtualCores shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"container_cores": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.virtualCores shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_cores" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"container_cores": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"container_cores": ${Int.MaxValue + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"container_cores": "256"}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for container_nodes" taggedAs (LocalTest) in {
    var resConfig = """{"container_nodes": ["1", "2"]}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerNodes shouldBe List("1", "2")

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_nodes" taggedAs (LocalTest) in {
    // 型違い
    var resConfig = """{"container_nodes": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success recource config for container_racks" taggedAs (LocalTest) in {
    var resConfig = """{"container_racks": ["1", "2"]}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerRacks shouldBe List("1", "2")

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_racks" taggedAs (LocalTest) in {
    // 型違い
    var resConfig = """{"container_racks": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  "getSourceStatus()" should "success source status" taggedAs (LocalTest) in {
    // データなし
    var stsMap = service.getSourcesStatus()
    stsMap.isEmpty shouldBe true

    // データ1件
    val processor = new HybridProcessor(sc, service.sqlc, "file://data/shogun_data.json", List("dummy"),
      RunMode.Development, "")
    service.sources.put("testData", (processor, None))
    stsMap = service.getSourcesStatus()
    println(s"sourceStatus: $stsMap")
    stsMap.size shouldBe 1
    stsMap.get("testData") match {
      case Some(data) => checkSourceStatus(data.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }

    // データ2件
    service.sources.putIfAbsent("testData2", (processor, None))
    stsMap = service.getSourcesStatus()
    println(s"sourceStatus: $stsMap")
    stsMap.size shouldBe 2
    stsMap.get("testData") match {
      case Some(data) => checkSourceStatus(data.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }
    stsMap.get("testData2") match {
      case Some(data) => checkSourceStatus(data.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }

    service.sources.clear()
  }

  "getModelsStatus()" should "success models status" taggedAs (LocalTest) in {
    val testService = new JubaQLService(sc, RunMode.Test, "file:///tmp/spark")
    val method = PrivateMethod[Map[String, Any]]('getModelsStatus)

    // モデルなし
    var stsMap = testService invokePrivate method()
    stsMap.isEmpty shouldBe true

    // モデル1件(resourceConfigなし)
    val cm = new CreateModel("CLASSIFIER", "test", None, null, "config", None)
    val juba = new TestJubatusApplication("Test", LearningMachineType.Classifier)
    testService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    stsMap = testService invokePrivate method()
    println(s"modelStatus: $stsMap")
    stsMap.size shouldBe 1
    stsMap.get("test") match {
      case Some(model) => checkModelStatus(model.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }

    // モデル2件(resourceConfigあり)
    val cm2 = new CreateModel("CLASSIFIER", "test2", None, null, "config", Option("resourceConfig"))
    testService.models.put("test2", (juba, cm2, LearningMachineType.Classifier))
    stsMap = testService invokePrivate method()
    println(s"modelStatus: $stsMap")
    stsMap.size shouldBe 2
    stsMap.get("test") match {
      case Some(model) => checkModelStatus(model.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }
    stsMap.get("test2") match {
      case Some(model) => checkModelStatus(model.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }
  }

  "getProcessorStatus()" should "success processor status" taggedAs (LocalTest) in {
    val stsMap = service.getProcessorStatus()
    stsMap.isEmpty shouldBe false
    println(s"processorStatus: $stsMap")
    stsMap.get("applicationId") should not be None
    stsMap.get("startTime") should not be None
    stsMap.get("currentTime") should not be None
    stsMap.get("opratingTime") should not be None
    stsMap.get("virtualMemory") should not be None
    stsMap.get("usedMemory") should not be None
  }

  private def checkSourceStatus(status: LinkedHashMap[String, Any]): Unit = {
    status.get("state") should not be None

    status.get("storage") match {
      case Some(storage) =>
        val storageMap = storage.asInstanceOf[LinkedHashMap[String, Any]]
        storageMap.get("path") should not be None
      case _ => fail()
    }

    status.get("stream") match {
      case Some(storage) =>
        val storageMap = storage.asInstanceOf[LinkedHashMap[String, Any]]
        storageMap.get("path") should not be None
      case _ => fail()
    }
  }

  private def checkModelStatus(status: LinkedHashMap[String, Any]): Unit = {
    status.get("learningMachineType") should not be None

    status.get("config") match {
      case Some(config) =>
        val configMap = config.asInstanceOf[LinkedHashMap[String, Any]]
        configMap.get("jubatusConfig") should not be None
        configMap.get("resourceConfig") should not be None
      case _ => fail()
    }

    status.get("jubatusYarnApplicationStatus") match {
      case Some(appStatus) =>
        val appMap = appStatus.asInstanceOf[LinkedHashMap[String, Any]]
        appMap.get("jubatusProxy") should not be None
        appMap.get("jubatusServers") should not be None
        appMap.get("jubatusOnYarn") should not be None
      case _ => fail()
    }
  }

  "takeAction():Status" should "return StatusResponse" taggedAs (LocalTest) in {
    val testService = new JubaQLService(sc, RunMode.Test, "file:///tmp/spark")
    val method = PrivateMethod[Either[(Int, String), JubaQLResponse]]('takeAction)

    val processor = new HybridProcessor(sc, testService.sqlc, "file://data/shogun_data.json", List("dummy"),
      RunMode.Test, "")
    testService.sources.put("testData", (processor, None))

    val cm = new CreateModel("CLASSIFIER", "test", None, null, "config", None)
    val juba = new TestJubatusApplication("Test", LearningMachineType.Classifier)
    testService.models.put("test", (juba, cm, LearningMachineType.Classifier))

    val ast: JubaQLAST = new Status()
    val result = testService invokePrivate method(ast)

    val sp = result.right.value.asInstanceOf[StatusResponse]
    println(s"result: $sp")
    sp.result shouldBe "STATUS"
    sp.sources.size shouldBe 1
    sp.models.size shouldBe 1
    sp.processor.isEmpty shouldBe false
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
