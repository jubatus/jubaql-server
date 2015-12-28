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
import us.jubat.yarn.common.{LearningMachineType, ServerConfig, ProxyConfig, Mixer, LogConfig}
import us.jubat.yarn.client.{JubatusYarnApplication, Resource, JubatusYarnApplicationStatus}
import scala.collection.mutable.LinkedHashMap
import scala.collection.Map
import scala.concurrent._
import scala.concurrent.duration.Duration

import scala.util.{Try, Success}

import java.net.URI


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
    override def convertLogConfig(logConfigJsonString: String): Either[(Int, String), LogConfig] =
      super.convertLogConfig(logConfigJsonString)
    override def complementServerConfig(serverJsonString: Option[String]): Either[(Int, String), ServerConfig] =
      super.complementServerConfig(serverJsonString)

    override def complementProxyConfig(proxyJsonString: Option[String]): Either[(Int, String), ProxyConfig] =
      super.complementProxyConfig(proxyJsonString)

    override def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] =
      super.takeAction(ast)

    override def getSourcesStatus(): Map[String, Any] =
      super.getSourcesStatus()

    override def getModelsStatus(): Map[String, Any] =
      super.getModelsStatus()

    override def getProcessorStatus(): Map[String, Any] =
      super.getProcessorStatus()

    override def queryUpdateWith(updateWith: UpdateWith): Either[(Int, String), String] =
      super.queryUpdateWith(updateWith)

    override def readConfigFile(configPath: URI): String =
      super.readConfigFile(configPath)
  }

  // create a subclass to test the protected methods for Production Mode
  class JubaQLServiceProductionTester(sc: SparkContext, runMode: RunMode) extends JubaQLService(sc, runMode, "file:///tmp/spark") {
    override def takeAction(ast: JubaQLAST): Either[(Int, String), JubaQLResponse] =
      super.takeAction(ast)
    override def getModelsStatus(): Map[String, Any] =
      super.getModelsStatus()
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error invalid model path2 for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:/tmp/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of saveModel method for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  it should "return success for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path2 for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:/home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of saveModel method for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new SaveModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error invalid model path2 for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:/tmp/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of loadModel method for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "file:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester(JubaQLServiceHelperSpec.throwExceptionName)

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast)
    service.models.remove("test")

    result.left.value._1 shouldBe 500
  }

  it should "return success for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of invalid model path2 for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:/home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    proService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast)
    proService.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "return error of loadModel method for Production mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val ast: JubaQLAST = new LoadModel("test", "hdfs:///home/data/models", "test001")
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left(""))
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
        value.containerNodes shouldBe List.empty
        value.containerRacks shouldBe List.empty
        value.containerCount shouldBe 2

      case _ =>
        fail()
    }
    // 必要なキーなし
    var resConfig = """{}""".stripMargin.trim
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
        value.containerNodes shouldBe List.empty
        value.containerRacks shouldBe List.empty
        value.containerCount shouldBe 2

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
    // 不正キー
    resConfig = """{"thread": 3, "test":0}""".stripMargin.trim
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
    resConfig = s"""{"applicationmaster_memory": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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
    resConfig = s"""{"jubatus_proxy_memory": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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
    resConfig = s"""{"applicationmaster_cores": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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

  it should "success recource config for container_count" taggedAs (LocalTest) in {
    var resConfig = """{"container_count": 3}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerCount shouldBe 3

      case _ =>
        fail()
    }

    // 最小値
    resConfig = """{"container_count": 1}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerCount shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    resConfig = s"""{"container_count": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[Resource] shouldBe true
        value.containerCount shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error recource config for container_count" taggedAs (LocalTest) in {
    // 範囲外
    var resConfig = """{"container_count": 0}""".stripMargin.trim
    var result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    resConfig = s"""{"container_count": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementResource(Option(resConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    resConfig = """{"container_count": "3"}""".stripMargin.trim
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
    resConfig = s"""{"container_priority": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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
    resConfig = s"""{"container_memory": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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
    resConfig = s"""{"jubatus_server_memory": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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
    resConfig = s"""{"container_cores": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
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

  "convertLogConfig()" should "success log config" taggedAs (LocalTest) in {
    val logConfig = """{"yarn_am": "hdfs:///jubatus-on-yarn/test/am_log4j.xml", "jubatus_proxy": "hdfs:///jubatus-on-yarn/test/jp_log4j.xml", "jubatus_server": "hdfs:///jubatus-on-yarn/test/js_log4j.xml"}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(Some("hdfs:///jubatus-on-yarn/test/am_log4j.xml"),Some("hdfs:///jubatus-on-yarn/test/jp_log4j.xml"),Some("hdfs:///jubatus-on-yarn/test/js_log4j.xml"))
      case Left((errCode, errMsg)) =>
        fail()
    }
  }

  it should "success log config empty string" taggedAs (LocalTest) in {
    val logConfig = "{}"
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(None, None, None)
      case Left((errCode, errMsg)) =>
        fail()
    }
  }

  it should "error log config illegal json format" taggedAs (LocalTest) in {
    val logConfig = ""
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        println(value)
        fail()
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }
  it should "success log config yarn_am: no value" taggedAs (LocalTest) in {
    val logConfig = """{"yarn_am": ""}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(Some(""),None,None)
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "success log config yarn_am: no file" taggedAs (LocalTest) in {
    val logConfig = """{"yarn_am": "hdfs:///dummy_log4j.xml"}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(Some("hdfs:///dummy_log4j.xml"),None,None)
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "error log config yarn_am: illegal jobject" taggedAs (LocalTest) in {
    val logConfig = """{"yarn_am": ["hdfs:///jubatus-on-yarn/test/am1_log4j.xml", "hdfs:///jubatus-on-yarn/test/am2_log4j.xml"]}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        println(value)
        fail()
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }
  it should "success log config jubatus_proxy: no value" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_proxy": ""}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(None,Some(""),None)
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "success log config jubatus_proxy: no file" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_proxy": "hdfs:///dummy_log4j.xml"}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(None,Some("hdfs:///dummy_log4j.xml"),None)
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "error log config jubatus_proxy: illegal jobject" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_proxy": true}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        println(value)
        fail()
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }
  it should "success log config jubatus_server: no value" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_server": ""}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(None,None,Some(""))
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "success log config jubatus_server: no file" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_server": "hdfs:///dummy_log4j.xml"}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        value shouldBe new LogConfig(None,None,Some("hdfs:///dummy_log4j.xml"))
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        fail()
    }
  }
  it should "error log config jubatus_server: illegal jobject" taggedAs (LocalTest) in {
    val logConfig = """{"jubatus_server": 123}""".stripMargin.trim
    val result = service.convertLogConfig(logConfig)
    result match {
      case Right(value) =>
        println(value)
        fail()
      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  "readConfig()" should "success no schema (use defaultFS)" in {
    try {
      val configString = service.readConfigFile(new URI("/jubatus-on-yarn/test/config.json"))
      configString shouldBe """{"method": "AROW","parameter": {"regularization_weight" : 1.0}}"""

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  it should "failed non support schema" in {
    try {
      service.readConfigFile(new URI("http:///tmp/config.json"))
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.io.IOException])
        e.getMessage should include regex "No FileSystem for scheme: http"
    }
  }

  it should "failed no file" in {
    try {
      service.readConfigFile(new URI("file:///noexist/config.json"))
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.io.FileNotFoundException])
        e.getMessage should include regex "File.*does not exist"
    }
  }

  it should "success read local file" in {
    val currentDir = new java.io.File(".").getCanonicalPath
    val configString = service.readConfigFile(new URI(s"file://${currentDir}/src/test/resources/config.json"))
    configString shouldBe """{"method": "AROW","parameter": {"regularization_weight" : 1.0}}"""
  }

  it should "success read hdfs file" in {
    val configString = service.readConfigFile(new URI("hdfs:///jubatus-on-yarn/test/config.json"))
    configString shouldBe """{"method": "AROW","parameter": {"regularization_weight" : 1.0}}"""
  }
  it should "success read multi-line file" in {
    val currentDir = new java.io.File(".").getCanonicalPath
    val configString = service.readConfigFile(new URI(s"file://${currentDir}/src/test/resources/multiline_config.json"))
    configString should fullyMatch regex """\{.*"method": "AROW",.*"parameter":.*\{"regularization_weight" : 1.0\}\}"""
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

    // モデル1件(resourceConfig/logConfig/serverConfig/proxyConfigなし)
    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left("config"), None, None, None)
    val juba = new TestJubatusApplication("Test", LearningMachineType.Classifier)
    testService.models.put("test", (juba, cm, LearningMachineType.Classifier))
    stsMap = testService invokePrivate method()
    println(s"modelStatus: $stsMap")
    stsMap.size shouldBe 1
    stsMap.get("test") match {
      case Some(model) => checkModelStatus(model.asInstanceOf[LinkedHashMap[String, Any]])
      case _ => fail()
    }

    // モデル2件(resourceConfig/logConfig/serverConfig/proxyConfigあり)
    val cm2 = new CreateModel("CLASSIFIER", "test2", None, null, Left("config"),
      Option(Left("resourceConfigString")), Option(Left("logConfigString")),Option(Left("serverConfigString")), Option(Left("proxyConfigString")))
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
        configMap.get("logConfig") should not be None
        configMap.get("serverConfig") should not be None
        configMap.get("proxyConfig") should not be None
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

    val cm = new CreateModel("CLASSIFIER", "test", None, null, Left("config"), None)
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

  "complementServerConfig()" should "success server config" taggedAs (LocalTest) in {
    // 指定なし
    var result = service.complementServerConfig(None)
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.thread shouldBe 2
        value.timeout shouldBe 10
        value.mixer shouldBe Mixer.Linear
        value.intervalSec shouldBe 16
        value.intervalCount shouldBe 512
        value.zookeeperTimeout shouldBe 10
        value.interconnectTimeout shouldBe 10

      case _ =>
        fail()
    }
    // 必要なキーなし
    var serverConfig = """{}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.thread shouldBe 2
        value.timeout shouldBe 10
        value.mixer shouldBe Mixer.Linear
        value.intervalSec shouldBe 16
        value.intervalCount shouldBe 512
        value.zookeeperTimeout shouldBe 10
        value.interconnectTimeout shouldBe 10

      case _ =>
        fail()
    }
  }

  it should "error server config for invalid format" taggedAs (LocalTest) in {
    var serverConfig = """""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    serverConfig = """{"thread", "timeout"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    serverConfig = """{thread: 3, timeout: 0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    serverConfig = """{"thread": 3, 0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
    // 不正キー
    serverConfig = """{"thread": 3, "test":0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for thread" taggedAs (LocalTest) in {
    var serverConfig = """{"thread": 3}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.thread shouldBe 3

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"thread": 1}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.thread shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"thread": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.thread shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for thread" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"thread": 0}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"thread": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"thread": "3"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for timeout" taggedAs (LocalTest) in {
    var serverConfig = """{"timeout": 30}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.timeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"timeout": 0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.timeout shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.timeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for timeout" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"timeout": -1}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"timeout": "30"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for mixer" taggedAs (LocalTest) in {
    var serverConfig = """{"mixer": "linear_mixer"}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.mixer shouldBe Mixer.Linear

      case _ =>
        fail()
    }

    serverConfig = """{"mixer": "random_mixer"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.mixer shouldBe Mixer.Random

      case _ =>
        fail()
    }

    serverConfig = """{"mixer": "broadcast_mixer"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.mixer shouldBe Mixer.Broadcast

      case _ =>
        fail()
    }

    serverConfig = """{"mixer": "skip_mixer"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.mixer shouldBe Mixer.Skip

      case _ =>
        fail()
    }
  }

  it should "error server config for mixer" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = s"""{"mixer": "test"}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"mixer": random_mixer}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for interval_sec" taggedAs (LocalTest) in {
    var serverConfig = """{"interval_sec": 10}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalSec shouldBe 10

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"interval_sec": 0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalSec shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"interval_sec": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalSec shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for interval_sec" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"interval_sec": -1}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"interval_sec": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"interval_sec": "10"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for interval_count" taggedAs (LocalTest) in {
    var serverConfig = """{"interval_count": 1024}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalCount shouldBe 1024

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"interval_count": 0}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalCount shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"interval_count": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.intervalCount shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for interval_count" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"interval_count": -1}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"interval_count": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"interval_count": "1024"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for zookeeper_timeout" taggedAs (LocalTest) in {
    var serverConfig = """{"zookeeper_timeout": 30}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.zookeeperTimeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"zookeeper_timeout": 1}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.zookeeperTimeout shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"zookeeper_timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.zookeeperTimeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for zookeeper_timeout" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"zookeeper_timeout": 0}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"zookeeper_timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"zookeeper_timeout": "30"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success server config for interconnect_timeout" taggedAs (LocalTest) in {
    var serverConfig = """{"interconnect_timeout": 30}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.interconnectTimeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    serverConfig = """{"interconnect_timeout": 1}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.interconnectTimeout shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    serverConfig = s"""{"interconnect_timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ServerConfig] shouldBe true
        value.interconnectTimeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error server config for interconnect_timeout" taggedAs (LocalTest) in {
    // 範囲外
    var serverConfig = """{"interconnect_timeout": 0}""".stripMargin.trim
    var result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    serverConfig = s"""{"interconnect_timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    serverConfig = """{"interconnect_timeout": "30"}""".stripMargin.trim
    result = service.complementServerConfig(Option(serverConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  "complementProxyConfig()" should "success proxy config" taggedAs (LocalTest) in {
    // 指定なし
    var result = service.complementProxyConfig(None)
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.thread shouldBe 4
        value.timeout shouldBe 10
        value.zookeeperTimeout shouldBe 10
        value.interconnectTimeout shouldBe 10
        value.poolExpire shouldBe 60
        value.poolSize shouldBe 0

      case _ =>
        fail()
    }
    // 必要なキーなし
    var proxyConfig = """{}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.thread shouldBe 4
        value.timeout shouldBe 10
        value.zookeeperTimeout shouldBe 10
        value.interconnectTimeout shouldBe 10
        value.poolExpire shouldBe 60
        value.poolSize shouldBe 0

      case _ =>
        fail()
    }
  }

  it should "error proxy config for invalid format" taggedAs (LocalTest) in {
    var proxyConfig = """""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    proxyConfig = """{"thread", "timeout"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    proxyConfig = """{thread: 3, timeout: 0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    proxyConfig = """{"thread": 3, 0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
    // 不正キー
    proxyConfig = """{"thread": 3, "test":0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for thread" taggedAs (LocalTest) in {
    var proxyConfig = """{"thread": 3}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.thread shouldBe 3

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"thread": 1}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.thread shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"thread": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.thread shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for thread" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"thread": 0}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"thread": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"thread": "3"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for timeout" taggedAs (LocalTest) in {
    var proxyConfig = """{"timeout": 30}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.timeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"timeout": 0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.timeout shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.timeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for timeout" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"timeout": -1}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"timeout": "30"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for zookeeper_timeout" taggedAs (LocalTest) in {
    var proxyConfig = """{"zookeeper_timeout": 30}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.zookeeperTimeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"zookeeper_timeout": 1}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.zookeeperTimeout shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"zookeeper_timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.zookeeperTimeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for zookeeper_timeout" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"zookeeper_timeout": 0}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"zookeeper_timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"zookeeper_timeout": "30"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for interconnect_timeout" taggedAs (LocalTest) in {
    var proxyConfig = """{"interconnect_timeout": 30}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.interconnectTimeout shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"interconnect_timeout": 1}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.interconnectTimeout shouldBe 1

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"interconnect_timeout": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.interconnectTimeout shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for interconnect_timeout" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"interconnect_timeout": 0}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"interconnect_timeout": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"interconnect_timeout": "30"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for pool_expire" taggedAs (LocalTest) in {
    var proxyConfig = """{"pool_expire": 30}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolExpire shouldBe 30

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"pool_expire": 0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolExpire shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"pool_expire": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolExpire shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for pool_expire" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"pool_expire": -1}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"pool_expire": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"pool_expire": "30"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  it should "success proxy config for pool_size" taggedAs (LocalTest) in {
    var proxyConfig = """{"pool_size": 10}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolSize shouldBe 10

      case _ =>
        fail()
    }

    // 最小値
    proxyConfig = """{"pool_size": 0}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolSize shouldBe 0

      case _ =>
        fail()
    }

    // 最大値
    proxyConfig = s"""{"pool_size": ${Int.MaxValue}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        value.isInstanceOf[ProxyConfig] shouldBe true
        value.poolSize shouldBe Int.MaxValue

      case _ =>
        fail()
    }
  }

  it should "error proxy config for pool_size" taggedAs (LocalTest) in {
    // 範囲外
    var proxyConfig = """{"pool_size": -1}""".stripMargin.trim
    var result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 範囲外
    proxyConfig = s"""{"pool_size": ${Int.MaxValue.toLong + 1}}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }

    // 型違い
    proxyConfig = """{"pool_size": "10"}""".stripMargin.trim
    result = service.complementProxyConfig(Option(proxyConfig))
    result match {
      case Right(value) =>
        fail()

      case Left((errCode, errMsg)) =>
        println(s"$errMsg")
        errCode shouldBe 400
    }
  }

  "takeAction():CreateModel" should "return an success for Development mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser

    // プロキシ設定なし
    var ast: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH unigram CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'
        SERVER CONFIG '{"thread": 2}'
      """.stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = service.takeAction(ast.get)
    result match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result shouldBe "CREATE MODEL (started)"
        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            Await.ready(jubaFut, Duration.Inf)
            jubaFut.value match {
              case Some(Success(j)) =>
                Await.ready(j.stop(), Duration.Inf)
            }
        }
      case _ =>
        fail()
    }

    // プロキシ設定あり
    ast = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH unigram CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'
        SERVER CONFIG '{"thread": 2}' PROXY CONFIG '{"thread": 2}'
      """.stripMargin)
    cm = ast.get.asInstanceOf[CreateModel]
    result = service.takeAction(ast.get)
    result match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result shouldBe "CREATE MODEL (started)\n - PROXY CONFIG setting has been ignored in Development mode"
        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            Await.ready(jubaFut, Duration.Inf)
            jubaFut.value match {
              case Some(Success(j)) =>
                Await.ready(j.stop(), Duration.Inf)
            }
        }
      case _ =>
        fail()
    }
  }

  it should "return an success with CONFIG" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
          | CONFIG '{"method": "AROW","parameter": {"regularization_weight" : 1.0}}'
          | RESOURCE CONFIG '{"applicationmaster_memory": 256}'
          | LOG CONFIG '{"yarn_am": "hdfs:///jubatus-on-yarn/test/am_log4j.xml"}'
          | SERVER CONFIG '{"thread": 3}'
          | PROXY CONFIG '{"thread": 2}'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result should startWith ("CREATE MODEL (started)")
        println("getModelStatus :" + proService.getModelsStatus().get("config"))
        // ログを目視確認
        // JubatusYarnApplication startParamに指定した設定ファイルの内容が展開されていること
      case Left(err) =>
        println(err)
        fail()
    }
  }

  it should "return an success with CONFIG FILE" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result should startWith ("CREATE MODEL (started)")
        val start = System.currentTimeMillis()
        while (proService.getModelsStatus().get("config") == None && System.currentTimeMillis() < start + 30000 ) {
          Thread.sleep(1000)
        }
        println("getModelStatus :" + proService.getModelsStatus().get("config"))
        // ログを目視確認
        // JubatusYarnApplication startParamに指定した設定ファイルの内容が展開されていること
      case Left(err) =>
        println(err)
        fail()
    }
  }

  it should "return an failed with jubatus CONFIG FILE(no file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/noexist_config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "can not read CONFIG FILE"
    }
  }

  it should "return an failed with jubatus RESOURCE CONFIG FILE(no file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/noexist_resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "can not read RESOURCE CONFIG FILE"
    }
  }

  it should "return an failed with jubatus LOG CONFIG FILE(no file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/noexist_logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "can not read LOG CONFIG FILE"
    }
  }

  it should "return an failed with jubatus SERVER CONFIG FILE(no file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/noexist_serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "can not read SERVER CONFIG FILE"
    }
  }

  it should "return an failed with jubatus PROXY CONFIG FILE(no file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/noexist_proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "can not read PROXY CONFIG FILE"
    }
  }

  it should "return an failed with jubatus CONFIG FILE(illegal json format file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/illegal_config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "input config is not a JSON."
    }
  }
    it should "return an failed with jubatus RESOURCE CONFIG FILE(illegal json format file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/illegal_config.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "Resource config is not a JSON"
    }
  }

  it should "return an failed with jubatus LOG CONFIG FILE(illegal json format file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/illegal_config.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "log_config is not a JSON."
    }
  }

  it should "return an failed with jubatus SERVER CONFIG FILE(illegal json format file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/illegal_config.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/proxyConfig.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "Server config is not a JSON"
    }
  }

  it should "return an failed with jubatus PROXY CONFIG FILE(illegal json format file)" taggedAs (LocalTest) in {

    val parser = new JubaQLParser
    val currentDir = new java.io.File(".").getCanonicalPath
    var ast: Option[JubaQLAST] = parser.parse(
      s"""CREATE CLASSIFIER MODEL test1 (label: label) AS * WITH id
      | CONFIG FILE 'file://${currentDir}/src/test/resources/config.json'
      | RESOURCE CONFIG FILE 'file://${currentDir}/src/test/resources/resourceConfig.json'
      | LOG CONFIG FILE 'file://${currentDir}/src/test/resources/logConfig.json'
      | SERVER CONFIG FILE 'file://${currentDir}/src/test/resources/serverConfig.json'
      | PROXY CONFIG FILE 'file://${currentDir}/src/test/resources/illegal_config.json'""".stripMargin)
    var cm = ast.get.asInstanceOf[CreateModel]
    var result: Either[(Int, String), JubaQLResponse] = proService.takeAction(ast.get)
    result match {
      case Right(value) =>
        fail()
      case Left(err) =>
        err._1 shouldBe 400
        err._2 shouldBe "Proxy config is not a JSON"
    }
  }

  // UpdateWith test
  "takeAction():UpdateWith" should "return an success for Anomaly" taggedAs (LocalTest) in {
    val parser = new JubaQLParser

    val cmAst: Option[JubaQLAST] = parser.parse(
      """
      CREATE ANOMALY MODEL test1 AS * CONFIG '{"method": "lof", "parameter": {"nearest_neighbor_num" : 10,
      "reverse_nearest_neighbor_num": 30, "method": "euclid_lsh", "parameter": {"hash_num": 64,
      "table_num": 4, "probe_num": 64, "bin_width": 100, "seed": 1091, "retain_projection": false}}}'
      """.stripMargin)

    val cmResult: Either[(Int, String), JubaQLResponse] = service.takeAction(cmAst.get)
    cmResult match {
      case Right(value) =>
        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            Await.ready(jubaFut, Duration.Inf)
        }
      case _ =>
        fail()
    }

    val upAst: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL test1 USING add WITH '{"test1": 0, "test2": "aaaa", "test3": 1}'
      """.stripMargin)

    val upResult: Either[(Int, String), JubaQLResponse] = service.takeAction(upAst.get)
    upResult match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result shouldBe "UPDATE MODEL (id_with_score{id: 0, score: Infinity})"

        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            jubaFut.value match {
              case Some(Success(j)) =>
                Await.ready(j.stop(), Duration.Inf)
            }
        }
      case _ =>
        fail()
    }
  }

  it should "return an success for Classifier" taggedAs (LocalTest) in {
    val parser = new JubaQLParser

    val cmAst: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: label) AS name WITH unigram
      CONFIG '{"method": "AROW", "parameter": {"regularization_weight" : 1.0}}'
      """.stripMargin)

    val cmResult: Either[(Int, String), JubaQLResponse] = service.takeAction(cmAst.get)
    cmResult match {
      case Right(value) =>
        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            Await.ready(jubaFut, Duration.Inf)
        }
      case _ =>
        fail()
    }

    val upAst: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL test1 USING train WITH '{"label": "label1", "name": "name1"}'
      """.stripMargin)

    val upResult: Either[(Int, String), JubaQLResponse] = service.takeAction(upAst.get)
    upResult match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result shouldBe "UPDATE MODEL (1)"

        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            jubaFut.value match {
              case Some(Success(j)) =>
                Await.ready(j.stop(), Duration.Inf)
            }
        }
      case _ =>
        fail()
    }
  }

  it should "return an success for Recommender" taggedAs (LocalTest) in {
    val parser = new JubaQLParser

    val cmAst: Option[JubaQLAST] = parser.parse(
      """
      CREATE RECOMMENDER MODEL test1 (id: pname) AS * CONFIG '{"method": "inverted_index", "parameter": {}}'
      """.stripMargin)

    val cmResult: Either[(Int, String), JubaQLResponse] = service.takeAction(cmAst.get)
    cmResult match {
      case Right(value) =>
        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            Await.ready(jubaFut, Duration.Inf)
        }
      case _ =>
        fail()
    }

    val upAst: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL test1 USING update_row WITH '{"pname": "name1", "team": "aaa", "test": 1}'
      """.stripMargin)

    val upResult: Either[(Int, String), JubaQLResponse] = service.takeAction(upAst.get)
    upResult match {
      case Right(value) =>
        val sp = value.asInstanceOf[StatementProcessed]
        sp.result shouldBe "UPDATE MODEL (true)"

        service.startedJubatusInstances.get("test1") match {
          case Some((jubaFut, _, _)) =>
            jubaFut.value match {
              case Some(Success(j)) =>
                Await.ready(j.stop(), Duration.Inf)
            }
        }
      case _ =>
        fail()
    }
  }

  "queryUpdateWith()" should "error without model" taggedAs (LocalTest) in {
    val updateWith = new UpdateWith("test", "train", """{"label": "label1", "name": "namme1"}""")
    val result = service.queryUpdateWith(updateWith)
    result.left.value._1 shouldBe 400
  }

  it should "error model and method mismatch for Anomaly" taggedAs (LocalTest) in {
    val cm = new CreateModel("ANOMALY", "test", None, List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Anomaly))
    val updateWith = new UpdateWith("test", "train", """{"label": "label1", "name": "namme1"}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error model and method mismatch for Classifier" taggedAs (LocalTest) in {
    val cm = new CreateModel("CLASSIFIER", "test", None, List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val updateWith = new UpdateWith("test", "add", """{"label": "label1", "name": "namme1"}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error model and method mismatch for Recommender" taggedAs (LocalTest) in {
    val cm = new CreateModel("RECOMMENDER", "test", None, List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Recommender))
    val updateWith = new UpdateWith("test", "add", """{"label": "label1", "name": "namme1"}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error no 'label' in CreateModel for Classifier" taggedAs (LocalTest) in {
    val cm = new CreateModel("CLASSIFIER", "test", None, List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val updateWith = new UpdateWith("test", "train", """{"label": "label1", "name": "namme1"}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error no 'id' in CreateModel for Recommender" taggedAs (LocalTest) in {
    val cm = new CreateModel("RECOMMENDER", "test", None, List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Recommender))
    val updateWith = new UpdateWith("test", "update_row", """{"pname": "name1", "team": "aaa", "test": 1}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error no 'label' in learningData for Classifier" taggedAs (LocalTest) in {
    val cm = new CreateModel("CLASSIFIER", "test", Some(("label", "label")), List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Classifier))
    val updateWith = new UpdateWith("test", "train", """{"name": "name1"}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  it should "error no 'id' in learningData for Recommender" taggedAs (LocalTest) in {
    val cm = new CreateModel("RECOMMENDER", "test", Some(("id", "pname")), List(), Left(""))
    val juba = new LocalJubatusApplicationTester("test")

    service.models.put("test", (juba, cm, LearningMachineType.Recommender))
    val updateWith = new UpdateWith("test", "update_row", """{"team": "aaa", "test": 1}""")
    val result = service.queryUpdateWith(updateWith)
    service.models.remove("test")

    result.left.value._1 shouldBe 400
  }

  override protected def beforeAll(): Unit = {
    sc = new SparkContext("local[3]", "JubaQL Processor Test")
    service = new JubaQLServiceTester(sc)

    val hosts: List[(String, Int)] = List(("localhost", 2181))
    proService = new JubaQLServiceProductionTester(sc, RunMode.Production(hosts))
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
