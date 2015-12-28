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

import org.scalatest._
import us.jubat.yarn.common.LearningMachineType
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import java.net.InetSocketAddress

class LocalJubatusApplicationSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {

  private var dummyJubaServer: DummyJubatusServer = null

  val anomalyConfig = """{
  "method" : "lof",
  "parameter" : {
    "nearest_neighbor_num" : 10,
    "reverse_nearest_neighbor_num" : 30,
    "method" : "euclid_lsh",
    "parameter" : {
      "hash_num" : 64,
      "table_num" : 4,
      "seed" : 1091,
      "probe_num" : 64,
      "bin_width" : 100,
      "retain_projection" : false
    }
  },
  "converter" : {
    "string_filter_types" : {},
    "string_filter_rules" : [],
    "num_filter_types" : {},
    "num_filter_rules" : [],
    "string_types" : {},
    "string_rules" : [
      { "key" : "*", "type" : "str", "sample_weight" : "bin", "global_weight" : "bin" }
    ],
    "num_types" : {},
    "num_rules" : [
      { "key" : "*", "type" : "num" }
    ]
  }
}"""

  val classifierConfig = """{
  "method" : "AROW",
  "parameter" : {
    "regularization_weight" : 1.0
  },
  "converter" : {
    "string_filter_types" : {},
    "string_filter_rules" : [],
    "num_filter_types" : {},
    "num_filter_rules" : [],
    "string_types" : {},
    "string_rules" : [
      { "key" : "*", "type" : "str", "sample_weight" : "bin", "global_weight" : "bin" }
    ],
    "num_types" : {},
    "num_rules" : [
      { "key" : "*", "type" : "num" }
    ]
  }
}"""

  val recommenderConfig = """{
  "method": "lsh",
  "parameter" : {
    "hash_num" : 64
  },
  "converter" : {
    "string_filter_types": {},
    "string_filter_rules":[],
    "num_filter_types": {},
    "num_filter_rules": [],
    "string_types": {},
    "string_rules":[
      {"key" : "*", "type" : "str", "sample_weight":"bin", "global_weight" : "bin"}
    ],
    "num_types": {},
    "num_rules": [
      {"key" : "*", "type" : "num"}
    ]
  }
}"""

  "mkfifo" should "make a named pipe" taggedAs (LocalTest) in {
    import java.io._

    val namedPipePath = "/tmp/abcdefghijklmnopqrstuvwxyz"
    val namedPipe = new File(namedPipePath)
    try {
      val runtime = Runtime.getRuntime
      namedPipe.exists shouldBe false
      LocalJubatusApplication.mkfifo(namedPipePath, runtime)
      namedPipe.exists shouldBe true

      val process = runtime.exec(s"file $namedPipePath")
      val in = process.getInputStream
      val br = new BufferedReader(new InputStreamReader(in))
      val line = br.readLine
      line should not be null
      // The following code depends on file command,
      // so may fail on some environment.
      line.contains("named pipe") shouldBe true
    } finally {
      namedPipe.delete()
    }
  }

  "jubaanomaly" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("foo", LearningMachineType.Anomaly, anomalyConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  "jubaclassifier" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  it should "be startable twice" taggedAs (LocalTest, JubatusTest) in {
    val f1 = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f1, Duration.Inf)
    val result1 = f1.value.get
    val f2 = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f2, Duration.Inf)
    val result2 = f2.value.get
    result1 shouldBe a[Success[_]]
    result2 shouldBe a[Success[_]]
    result1.get.jubatusProxy.port shouldBe 9199
    result2.get.jubatusProxy.port shouldBe 9200
    result1 match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
    result2 match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  "jubarecommender" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("baz", LearningMachineType.Recommender, recommenderConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  "saveModel" should "saveModel for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test001", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val dstFile = new java.io.File("/tmp/t1/data/classifier/test001/0.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }
    val dstPath = new java.io.File("/tmp/t1/data/classifier/test001")
    if (dstPath.exists()) {
      dstPath.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.saveModel(modelPath, "test001")

    retApp shouldBe a[Success[_]]
    dstFile.exists() shouldBe true
  }

  it should "saveModel a file doesn't exist for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test002", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val dstFile = new java.io.File("/tmp/t1/data/classifier/test002/0.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }
    val dstPath = new java.io.File("/tmp/t1/data/classifier/test002")
    if (!dstPath.exists()) {
      dstPath.mkdirs()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.saveModel(modelPath, "test002")

    retApp shouldBe a[Success[_]]
    dstFile.exists() shouldBe true
  }

  it should "saveModel a file exists for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test003", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val dstPath = new java.io.File("/tmp/t1/data/classifier/test003")
    if (!dstPath.exists()) {
      dstPath.mkdirs()
    }

    val dstFile = new java.io.File("/tmp/t1/data/classifier/test003/0.jubatus")
    if (!dstFile.exists()) {
      dstFile.createNewFile()
    }

    val writer = new java.io.FileWriter(dstFile, true)
    writer.write("test")
    writer.close()
    val beforLen = dstFile.length()

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.saveModel(modelPath, "test003")

    retApp shouldBe a[Success[_]]
    dstFile.exists() shouldBe true
    dstFile.length() should not be beforLen
  }

  it should "saveModel no write permission for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test004", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val dstPath = new java.io.File("/tmp/t1/data/classifier/test004")
    if (!dstPath.exists()) {
      dstPath.mkdirs()
    }
    val dstFile = new java.io.File("/tmp/t1/data/classifier/test004/0.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }
    dstPath.setReadOnly()

    val modelPath = new Path("file:///t1/data/classifier")
    val retApp = juba.saveModel(modelPath, "test004")

    retApp should not be a[Success[_]]
    dstFile.exists() shouldBe false
  }

  it should "saveModel relative path for classifier" taggedAs (LocalTest, JubatusTest) in {
    saveModelForRelativePath("tmp/data/classifier", "test005")
  }

  it should "saveModel relative path2 for classifier" taggedAs (LocalTest, JubatusTest) in {
    saveModelForRelativePath("./tmp/data/classifier", "test006")
  }

  it should "saveModel relative path3 for classifier" taggedAs (LocalTest, JubatusTest) in {
    saveModelForRelativePath("../tmp/data/classifier", "test007")
  }

  it should "saveModel save RPC result.size is 0" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, dummyJubaServer.JubaServer.resultSize0, LearningMachineType.Classifier, "jubaclassifier", 9300)
    val modelPath = new Path("file:///tmp/data/classifier")
    val retApp = juba.saveModel(modelPath, "test001")

    retApp shouldBe a[Failure[_]]

    retApp match {
      case Failure(t) =>
        t.printStackTrace()

      case Success(_) =>
        printf("save model success")
    }
  }

  it should "saveModel save RPC result.size is 2" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, dummyJubaServer.JubaServer.resultSize2, LearningMachineType.Classifier, "jubaclassifier", 9300)
    val modelPath = new Path("file:///tmp/data/classifier")
    val retApp = juba.saveModel(modelPath, "test001")

    retApp shouldBe a[Failure[_]]

    retApp match {
      case Failure(t) =>
        t.printStackTrace()

      case Success(_) =>
        printf("save model success")
    }
  }

  private def saveModelForRelativePath(path: String, id: String) {
    val juba = new LocalJubatusApplication(null, id, LearningMachineType.Classifier, "jubaclassifier", 9300)

    val dstFile = new java.io.File(s"$path/$id/0.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }

    val df = new java.io.File(s"/tmp/data/classifier/$id/0.jubatus")
    if (df.exists()) {
      df.delete()
    }

    val modelPath = new Path(s"file://$path")
    val retApp = juba.saveModel(modelPath, id)

    retApp shouldBe a[Success[_]]
    dstFile.exists() shouldBe true
    df.exists() shouldBe false
  }

  "loadModel" should "loadModel for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test001", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test001")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test001/0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File("/tmp/dummyHost_port_classifier_test001.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test001")

    retApp shouldBe a[Success[_]]
    srcFile.exists() shouldBe true
    dstFile.exists() shouldBe true
  }

  it should "loadModel a dstFile exists for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test002", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test002")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test002/0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File("/tmp/dummyHost_port_classifier_test002.jubatus")
    if (!dstFile.exists()) {
      dstFile.createNewFile()
    }
    val writer = new java.io.FileWriter(dstFile, true)
    writer.write("test")
    writer.close()
    val beforLen = dstFile.length()

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test002")

    retApp shouldBe a[Success[_]]
    srcFile.exists() shouldBe true
    dstFile.exists() shouldBe true
    dstFile.length() should not be beforLen
  }

  it should "loadModel a srcFolder doesn't exists for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test003", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test003")
    if (srcPath.exists()) {
      FileUtils.cleanDirectory(srcPath)
      srcPath.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test003")

    retApp shouldBe a[Failure[_]]
    retApp match {
      case Failure(t) =>
        t.printStackTrace()
      case Success(_) =>
        printf("load model success")
    }
  }

  it should "loadModel a srcFile doesn't exists for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test004", LearningMachineType.Classifier, "jubaclassifier", 9300)

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test004")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test004/0.jubatus")
    if (srcFile.exists()) {
      srcFile.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test004")

    retApp shouldBe a[Failure[_]]
    retApp match {
      case Failure(t) =>
        t.printStackTrace()
      case Success(_) =>
        printf("load model success")
    }
  }

  it should "loadModel relative path for classifier" taggedAs (LocalTest, JubatusTest) in {
    loadModelForRelativePath("tmp/t1/data/classifier", "test005")
  }

  it should "loadModel relative path2 for classifier" taggedAs (LocalTest, JubatusTest) in {
    loadModelForRelativePath("./tmp/t1/data/classifier", "test006")
  }

  it should "loadModel relative path3 for classifier" taggedAs (LocalTest, JubatusTest) in {
    loadModelForRelativePath("../tmp/t1/data/classifier", "test007")
  }

  it should "loadModel getStatus RPC result.size is 0" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "errTest001", LearningMachineType.Classifier, "jubaclassifier", 9300)
    dummyJubaServer.statusType = dummyJubaServer.JubaServer.resultSize0

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test008")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test008/0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File("/tmp/dummyHost_port_classifier_test008.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test008")

    retApp shouldBe a[Failure[_]]
    retApp match {
      case Failure(t) =>
        t.printStackTrace()
      case Success(_) =>
        printf("load model success")
    }
  }

  it should "loadModel getStatus RPC result.size is 2" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "errTest001", LearningMachineType.Classifier, "jubaclassifier", 9300)
    dummyJubaServer.statusType = dummyJubaServer.JubaServer.resultSize2

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test008")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test008/0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File("/tmp/dummyHost_port_classifier_test008.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test008")

    retApp shouldBe a[Failure[_]]
    retApp match {
      case Failure(t) =>
        t.printStackTrace()
      case Success(_) =>
        printf("load model success")
    }
  }

  it should "loadModel load RPC result error" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "errTest001", LearningMachineType.Classifier, "jubaclassifier", 9300)
    dummyJubaServer.statusType = dummyJubaServer.JubaServer.resultSize1

    val srcPath = new java.io.File("/tmp/t1/data/classifier/test008")
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File("/tmp/t1/data/classifier/test008/0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File("/tmp/dummyHost_port_classifier_test008.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }

    val modelPath = new Path("file:///tmp/t1/data/classifier")
    val retApp = juba.loadModel(modelPath, "test008")

    retApp shouldBe a[Failure[_]]
    retApp match {
      case Failure(t) =>
        t.printStackTrace()
      case Success(_) =>
        printf("load model success")
    }
  }

  private def loadModelForRelativePath(path: String, id: String) {
    val juba = new LocalJubatusApplication(null, id, LearningMachineType.Classifier, "jubaclassifier", 9300)

    val localFileSystem = org.apache.hadoop.fs.FileSystem.getLocal(new Configuration())
    val srcDirectory = localFileSystem.pathToFile(new org.apache.hadoop.fs.Path(path))

    val srcPath = new java.io.File(srcDirectory, id)
    if (!srcPath.exists()) {
      srcPath.mkdirs()
    }
    val srcFile = new java.io.File(srcPath, "0.jubatus")
    if (!srcFile.exists()) {
      srcFile.createNewFile()
    }
    val dstFile = new java.io.File(s"/tmp/dummyHost_port_classifier_$id.jubatus")
    if (dstFile.exists()) {
      dstFile.delete()
    }
    val df = new java.io.File(s"/tmp/t1/data/classifier/$id/0.jubatus")
    if (df.exists()) {
      df.delete()
    }

    val modelPath = new Path(s"file://$path")
    val retApp = juba.loadModel(modelPath, id)

    retApp shouldBe a[Success[_]]
    srcFile.exists() shouldBe true
    dstFile.exists() shouldBe true
    df.exists() shouldBe false
  }

  "status" should "status for classifier" taggedAs (LocalTest, JubatusTest) in {
    val juba = new LocalJubatusApplication(null, "Test001", LearningMachineType.Classifier, "jubaclassifier", 9300)
    dummyJubaServer.statusType = dummyJubaServer.JubaServer.resultSize1

    val retStatus = juba.status
    retStatus.jubatusProxy shouldBe null
    retStatus.jubatusServers should not be null
    retStatus.yarnApplication shouldBe null

    retStatus.jubatusServers.size() shouldBe 1
  }

  override protected def beforeAll(): Unit = {
    dummyJubaServer = new DummyJubatusServer
    dummyJubaServer.start(9300)
  }

  override protected def afterAll(): Unit = {
    dummyJubaServer.stop()
  }

}

class DummyJubatusServer {
  var server: org.msgpack.rpc.Server = null
  var statusType: String = JubaServer.resultSize1

  object JubaServer {
    val resultSize0: String = "resultSize0"
    val resultSize1: String = "resultSize1"
    val resultSize2: String = "resultSize2"
  }
  class JubaServer {
    def save(strId: String): java.util.Map[String, String] = {
      var ret: java.util.Map[String, String] = new java.util.HashMap()

      strId match {
        case JubaServer.resultSize0 => // return 0
          ret

        case JubaServer.resultSize2 => // return 2
          ret.put("key1", "value1")
          ret.put("key2", "value2")
          ret

        case _ =>
          val file = new java.io.File("/tmp/test.jubatus")
          if (!file.exists()) {
            file.createNewFile()
          }
          ret.put("key1", "/tmp/test.jubatus")
          ret
      }
    }

    def load(strId: String): Boolean = {

      strId match {
        case "errTest001" =>
          false

        case _ =>
          true
      }
    }

    def get_status(): java.util.Map[String, java.util.Map[String, String]] = {
      var ret: java.util.Map[String, java.util.Map[String, String]] = new java.util.HashMap()
      var ret2: java.util.Map[String, String] = new java.util.HashMap()
      statusType match {
        case JubaServer.resultSize0 =>
          ret

        case JubaServer.resultSize2 =>
          ret2.put("datadir", "file:///tmp")
          ret2.put("type", "classifier")
          ret.put("key1", ret2)
          ret.put("key2", ret2)
          ret

        case _ =>
          ret2.put("datadir", "file:///tmp")
          ret2.put("type", "classifier")
          ret.put("dummyHost_port", ret2)
          ret
      }
    }
  }

  def start(id: Int) {
    server = new org.msgpack.rpc.Server()
    server.serve(new JubaServer())
    server.listen(new InetSocketAddress(id))
    println("*** DummyJubatusServer start ***")
  }

  def stop() {
    server.close()
    println("*** DummyJubatusServer stop ***")
  }
}
