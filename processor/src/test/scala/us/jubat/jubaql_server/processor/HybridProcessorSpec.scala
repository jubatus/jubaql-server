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

import java.util.Properties
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest._
import scala.collection.mutable.LinkedHashMap
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage


class HybridProcessorSpec
  extends FlatSpec
  with ShouldMatchers
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "HybridStreamSpec")
  val sqlc = new SQLContext(sc)

  "HybridProcessor" should "throw an exception for an invalid storage location" taggedAs (LocalTest) in {
    the[IllegalArgumentException] thrownBy {
      new HybridProcessor(sc, sqlc, "myLoc", Nil)
    } should have message "'myLoc' is not a valid storage specification"
  }

  it should "throw an exception for an invalid stream location" taggedAs (LocalTest) in {
    the[IllegalArgumentException] thrownBy {
      new HybridProcessor(sc, sqlc, "file:///tmp", "myLoc" :: Nil)
    } should have message "'myLoc' is not a valid stream specification"
  }

  it should "throw an exception for more than one stream location" taggedAs (LocalTest) in {
    the[IllegalArgumentException] thrownBy {
      new HybridProcessor(sc, sqlc, "file:///tmp", "myLoc" :: "yourLoc" :: Nil)
    } should have message ("requirement failed: " +
      "More than one stream location is not supported at the moment.")
  }

  "Static-only processing (empty source)" should "end when the processing is done" taggedAs (LocalTest) in {
    val startTime = System.nanoTime()
    val processor = new HybridProcessor(sc, sqlc, "empty", Nil)
    processor.startJValueProcessing(rdd => rdd.count)
    processor.state shouldBe Running
    processor.awaitTermination()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 5e9 // less than 5 seconds
  }

  "Static-only processing (local files)" should "end when the processing is done" taggedAs (LocalTest) in {
    val startTime = System.nanoTime()
    val processor = new HybridProcessor(sc, sqlc, "file://src/test/resources/dummydata", Nil)
    processor.startJValueProcessing(rdd => rdd.count)
    processor.state shouldBe Running
    processor.awaitTermination()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e9 // less than 10 seconds
  }

  it should "throw an exception when started while processing is running" taggedAs (LocalTest) in {
    val startTime = System.nanoTime()
    val processor = new HybridProcessor(sc, sqlc, "file://src/test/resources/dummydata", Nil)
    processor.startJValueProcessing(rdd => rdd.count)
    processor.state shouldBe Running
    a[RuntimeException] should be thrownBy {
      processor.startJValueProcessing(rdd => rdd.count)
    }
    processor.awaitTermination()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e9 // less than 10 seconds
  }

  it should "throw an exception when started again after finishing" taggedAs (LocalTest) in {
    val startTime = System.nanoTime()
    val processor = new HybridProcessor(sc, sqlc, "file://src/test/resources/dummydata", Nil)
    processor.startJValueProcessing(rdd => rdd.count)
    processor.state shouldBe Running
    processor.awaitTermination()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e9 // less than 10 seconds
    a[RuntimeException] should be thrownBy {
      processor.startJValueProcessing(rdd => rdd.count)
    }
  }

  "getStatus()" should "return datasource status for local files" taggedAs (LocalTest) in {
    val inPath = "file://src/test/resources/dummydata"
    val processor = new HybridProcessor(sc, sqlc, inPath, Nil)
    var status = processor.getStatus()

    // Initialized
    status.get("state") shouldBe Some("Initialized")

    status.get("storage") match {
      case Some(storage) =>
        val storageMap: LinkedHashMap[String, Any] = storage.asInstanceOf[LinkedHashMap[String, Any]]
        storageMap.get("path") shouldBe Some(inPath)
      case _ => fail()
    }

    status.get("stream") match {
      case Some(stream) =>
        val streamMap: LinkedHashMap[String, Any] = stream.asInstanceOf[LinkedHashMap[String, Any]]
        streamMap.get("path") shouldBe Some(List())
      case _ => fail()
    }

    // Running
    processor.startJValueProcessing(rdd => rdd.count)
    status = processor.getStatus()
    status.get("state") shouldBe Some("Running")

    // Finished
    processor.awaitTermination()
    status = processor.getStatus()
    status.get("state") shouldBe Some("Finished")
  }

  override def afterAll = {
    sc.stop()
  }
}

class HDFSStreamSpec
  extends FlatSpec
  with ShouldMatchers
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "HDFSStreamSpec")
  val sqlc = new SQLContext(sc)

  "HybridProcessor" should "accept valid HDFS paths" taggedAs (LocalTest) in {
    val hdfsPath = "hdfs:///tmp"
    noException should be thrownBy {
      new HybridProcessor(sc, sqlc, hdfsPath, Nil)
    }
  }

  it should "not accept invalid HDFS paths" taggedAs (LocalTest) in {
    val hdfsPath = "hdfs:/abcd//tmp"
    the[IllegalArgumentException] thrownBy {
      new HybridProcessor(sc, sqlc, hdfsPath, Nil)
    } should have message s"'$hdfsPath' is not a valid storage specification"
  }

  "HDFS-only processing on a populated directory" should "end when the processing is done" taggedAs (HDFSTest) in {
    val path = "hdfs:///user/fluentd/dummy"
    val processor = new HybridProcessor(sc, sqlc, path, Nil)
    processor.state shouldBe Initialized
    val startTime = System.nanoTime()
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running

    processor.awaitTermination()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 50e9 // less than 50 seconds
    val (staticInfo, streamInfo) = stopFun()
    // check number of items and received id
    staticInfo.itemCount should be > 10000L
    staticInfo.runtime should be > 0L
    staticInfo.maxId should not be empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime shouldBe 0L
    streamInfo.maxId shouldBe empty
  }

  it should "be manually stoppable" taggedAs (HDFSTest) in {
    val path = "hdfs:///user/fluentd/dummy"
    val processor = new HybridProcessor(sc, sqlc, path, Nil)
    processor.state shouldBe Initialized
    val startTime = System.nanoTime()
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    Thread.sleep(5000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    val executionTime = (System.nanoTime() - startTime)
    processor.state shouldBe Finished
    executionTime.toDouble should be < 50e9 // less than 50 seconds
    // check number of items and received id
    staticInfo.itemCount should be > 10000L
    staticInfo.runtime should be > 0L
    staticInfo.maxId should not be empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime shouldBe 0L
    streamInfo.maxId shouldBe empty
  }

  "HDFS-only processing on an empty directory" should "not process anything" taggedAs (HDFSTest) in {
    val path = "hdfs:///user/empty"
    val processor = new HybridProcessor(sc, sqlc, path, Nil)
    processor.state shouldBe Initialized
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    processor.awaitTermination()
    processor.state shouldBe Finished
    val (staticInfo, streamInfo) = stopFun()
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime shouldBe 0L
    streamInfo.maxId shouldBe empty
  }

  "getStatus()" should "return datasource status for hdfs files" taggedAs (HDFSTest) in {
    val inPath = "hdfs:///user/empty"
    val processor = new HybridProcessor(sc, sqlc, inPath, Nil)
    var status = processor.getStatus()

    // Initialized
    status.get("state") shouldBe Some("Initialized")

    status.get("storage") match {
      case Some(storage) =>
        val storageMap: LinkedHashMap[String, Any] = storage.asInstanceOf[LinkedHashMap[String, Any]]
        storageMap.get("path") shouldBe Some(inPath)
      case _ => fail()
    }

    status.get("stream") match {
      case Some(stream) =>
        val streamMap: LinkedHashMap[String, Any] = stream.asInstanceOf[LinkedHashMap[String, Any]]
        streamMap.get("path") shouldBe Some(List())
      case _ => fail()
    }

    // Running
    processor.startJValueProcessing(rdd => rdd.count)
    status = processor.getStatus()
    status.get("state") shouldBe Some("Running")

    // Finished
    processor.awaitTermination()
    status = processor.getStatus()
    status.get("state") shouldBe Some("Finished")
  }

  override def afterAll = {
    sc.stop()
  }
}

class KafkaStreamSpec
  extends FlatSpec
  with ShouldMatchers
  with HasKafkaPath
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "KafkaStreamSpec")
  val sqlc = new SQLContext(sc)

  "HybridProcessor" should "accept valid Kafka paths" taggedAs (LocalTest) in {
    val kafkaURI = s"kafka://$kafkaPath/dummy/1"
    noException should be thrownBy {
      new HybridProcessor(sc, sqlc, "empty", kafkaURI :: Nil)
    }
  }

  it should "not accept invalid Kafka paths" taggedAs (LocalTest) in {
    val kafkaURI = s"kafka://$kafkaPath/dummy/1/300"
    the[IllegalArgumentException] thrownBy {
      new HybridProcessor(sc, sqlc, "file:///tmp", kafkaURI :: Nil)
    } should have message s"'$kafkaURI' is not a valid stream specification"
  }

  "Kafka-only processing on a populated topic" should "be manually stoppable before starting" taggedAs (KafkaTest) in {
    val path = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    processor.state shouldBe Initialized
    val startTime = System.nanoTime()
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    Thread.sleep(1700) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    processor.awaitTermination()
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e9 // less than 10 seconds
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime shouldBe 0L
    streamInfo.maxId shouldBe empty
  }

  it should "be manually stoppable while running" taggedAs (KafkaTest) in {
    val path = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    processor.state shouldBe Initialized
    val startTime = System.nanoTime()
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    Thread.sleep(10000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e10 // less than 100 seconds
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount should be > 100L
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
  }

  it should "be processable using SQL" taggedAs (KafkaTest) in {
    val path = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    processor.state shouldBe Initialized
    val startTime = System.nanoTime()
    val schema = StructType(List(StructField("video_id", LongType, nullable = false),
      StructField("title", StringType, nullable = false)))
    import sqlc._
    val stopFun = processor.startTableProcessing(rdd => {
      rdd.registerTempTable("test")
      sql("SELECT video_id FROM test LIMIT 10").count
    }, Some(schema))._1
    processor.state shouldBe Running
    Thread.sleep(15000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 40e9 // less than 40 seconds
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount should be > 0L
    // even though we *select* only a couple of items, the number of
    // *seen* items will still be large, so we can't talk about an upper
    // bound of items here
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
  }

  "Kafka-only processing on an empty topic" should "not process anything" taggedAs (KafkaTest) in {
    Thread.sleep(2000)
    val path = s"kafka://$kafkaPath/notopic/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    processor.state shouldBe Initialized
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    Thread.sleep(10000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime should be > 0L
    streamInfo.maxId shouldBe empty
  }

  "getStatus()" should "return datasource status for stream" taggedAs (KafkaTest) in {
    val inPath = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", inPath :: Nil)
    var status = processor.getStatus()

    // Initialized
    status.get("state") shouldBe Some("Initialized")

    status.get("storage") match {
      case Some(storage) =>
        val storageMap: LinkedHashMap[String, Any] = storage.asInstanceOf[LinkedHashMap[String, Any]]
        storageMap.get("path") shouldBe Some("empty")
      case _ => fail()
    }

    status.get("stoream") match {
      case Some(stream) =>
        val streamMap: LinkedHashMap[String, Any] = stream.asInstanceOf[LinkedHashMap[String, Any]]
        streamMap.get("path") shouldBe Some(List(inPath))
      case _ => fail()
    }

    // Running
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    status = processor.getStatus()
    status.get("state") shouldBe Some("Running")

    Thread.sleep(1700) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()

    // Finished
    processor.awaitTermination()
    status = processor.getStatus()
    status.get("state") shouldBe Some("Finished")
  }

  override def afterAll = {
    sc.stop()
  }
}

class HDFSKafkaStreamSpec
  extends FlatSpec
  with ShouldMatchers
  with HasKafkaPath
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "HDFSKafkaStreamSpec")
  val sqlc = new SQLContext(sc)

  "HDFS+Kafka processing" should "change processing smoothly" taggedAs (HDFSTest, KafkaTest) in {
    val hdfsPath = "hdfs:///user/fluentd/dummy"
    val kafkaURI = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, hdfsPath, kafkaURI :: Nil)
    processor.state shouldBe Initialized
    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1
    processor.state shouldBe Running
    Thread.sleep(60000)
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    // check number of items and received id
    staticInfo.itemCount should be > 10000L
    staticInfo.runtime should be > 0L
    staticInfo.maxId should not be empty
    streamInfo.itemCount should be > 100L
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
    // we can't make a comparison such as "id x should be N larger than id y"
    // with string ids, but we can check one is larger than the other
    streamInfo.maxId.get should be > staticInfo.maxId.get
  }

  /* This test is ignored because
   * - due to some bug in our test setup, some KDD dummy data has slipped
   *   into the test data (just very few items, like < 1%)
   * - and this data has some IDs longer than Long in it, but they are
   *   not discovered by the schema inference process (since we look
   *   only at a small percentage of the data for schema inference)
   *   and so the schema is inferred as having Long at that place.
   *   When we hit a number longer than Long in the actual processing, we
   *   will get a cast exception for BigDecimal => Long.
   * This cannot really be worked around except for (fixing the bug in the
   * test setup and) increasing the ratio of looked-at items for schema
   * inference.
   */
  it should "be able to process SQL queries" taggedAs (HDFSTest, KafkaTest) ignore {
    val hdfsPath = "hdfs:///user/fluentd/dummy"
    val kafkaURI = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, hdfsPath, kafkaURI :: Nil)
    processor.state shouldBe Initialized
    import sqlc._
    val stopFun = processor.startTableProcessing(rdd => {
      rdd.registerTempTable("test")
      sql("SELECT title, description FROM test").count
    }, None)._1
    processor.state shouldBe Running
    Thread.sleep(60000)
    val (staticInfo, streamInfo) = stopFun()
    processor.state shouldBe Finished
    // check number of items and received id
    staticInfo.itemCount should be > 10000L
    staticInfo.runtime should be > 0L
    staticInfo.maxId should not be empty
    streamInfo.itemCount should be > 1000L
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
    // we can't make a comparison such as "id x should be N larger than id y"
    // with string ids, but we can check one is larger than the other
    streamInfo.maxId.get should be > staticInfo.maxId.get
  }

  override def afterAll = {
    sc.stop()
  }
}

class FileKafkaStreamSpec
  extends FlatSpec
  with ShouldMatchers
  with HasKafkaPath
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "FileKafkaStreamSpec")
  val sqlc = new SQLContext(sc)

  // テスト実行時の事前準備
  // ・kafka serverを起動する
  // ・dummy1,dummy_1,dummy_2のtopicが存在しない場合作成する
  "ProcessingStatus(phase/starTime/count/timeStamp)" should "check ProcessingStatus value" taggedAs (HDFSTest, KafkaTest) in {
    val filePath = "file://src/test/resources/test_data.json"
    val kafkaURI = s"kafka://$kafkaPath/dummy1/1"
    val processor = new HybridProcessor(sc, sqlc, filePath, kafkaURI :: Nil)

    val testStartTime = System.currentTimeMillis()

    processor.getStatus.get("process_phase").get shouldBe "Stop"
    var storageMap = processor.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMap.get("storage_start").get shouldBe 0L
    storageMap.get("storage_count").get shouldBe 0L
    var streamMap = processor.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMap.get("stream_start").get shouldBe 0L
    streamMap.get("stream_count").get shouldBe 0L
    processor.getStatus.get("process_timestamp").get shouldBe ""

    val stopFun = processor.startJValueProcessing(rdd => rdd.count)._1

    sendKafkaMessage(s"$kafkaServerAddress", "dummy1", Array("""{"label":"tokugawa", "name":"test1", "jubaql_timestamp": "2016-11-11T11:11:11"}""", """{"label":"tokugawa", "name":"test2", "jubaql_timestamp": "2014-11-11T11:11:15"}""","""{"label":"tokugawa", "name":"test3", "jubaql_timestamp": "2016-11-11T11:11:10"}"""))

    processor.getStatus.get("process_phase").get shouldBe "Storage"

    while (processor.phase == StoragePhase) {
      Thread.sleep(1000)
    }
    processor.getStatus.get("process_phase").get shouldBe "Stream"
    storageMap = processor.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMap.get("storage_start").get.asInstanceOf[Long]  should be > testStartTime
    storageMap.get("storage_count").get shouldBe 3L
    streamMap = processor.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMap.get("stream_start").get.asInstanceOf[Long] should be > storageMap.get("storage_start").get.asInstanceOf[Long]
    streamMap.get("stream_count").get shouldBe 0L
    processor.getStatus.get("process_timestamp").get shouldBe "2015-11-11T11:11:13"

    Thread.sleep(5000)
    val (staticInfo, streamInfo) = stopFun()

    processor.getStatus.get("process_phase").get shouldBe "Stop"
    storageMap = processor.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMap.get("storage_start").get.asInstanceOf[Long]  should be > testStartTime
    storageMap.get("storage_count").get shouldBe 3L
    streamMap = processor.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMap.get("stream_start").get.asInstanceOf[Long] should be > storageMap.get("storage_start").get.asInstanceOf[Long]
    streamMap.get("stream_count").get shouldBe 2L
    processor.getStatus.get("process_timestamp").get shouldBe "2016-11-11T11:11:11"
  }

  it should "Interference confirmation of ProcessingStatus values" taggedAs (HDFSTest, KafkaTest) in {
    val filePathA = "file://src/test/resources/data_1.json"
    val kafkaURIA = s"kafka://$kafkaPath/dummy_1/1"
    val processorA = new HybridProcessor(sc, sqlc, filePathA, kafkaURIA :: Nil)
    val filePathB = "file://src/test/resources/data_2.json"
    val kafkaURIB = s"kafka://$kafkaPath/dummy_2/1"
    val processorB = new HybridProcessor(sc, sqlc, filePathB, kafkaURIB :: Nil)

    sendKafkaMessage(s"$kafkaServerAddress", "dummy_1", Array("""{"label":"tokugawa", "name":"test1", "jubaql_timestamp": "2015-11-11T11:11:11"}""", """{"label":"tokugawa", "name":"test2", "jubaql_timestamp": "2015-11-11T11:11:12"}"""))
    Thread.sleep(3000)
    sendKafkaMessage(s"$kafkaServerAddress", "dummy_2", Array("""{"label":"tokugawa", "name":"test1", "jubaql_timestamp": "2016-11-11T11:11:11"}""", """{"label":"tokugawa", "name":"test1", "jubaql_timestamp": "2016-11-11T11:11:13"}"""))

    val stopFunA = processorA.startJValueProcessing(rdd => rdd.count)._1
    while (processorA.phase == StoragePhase) {
      Thread.sleep(1000)
    }
    Thread.sleep(10000)
    processorA.getStatus.get("process_phase").get shouldBe "Stream"
    var storageMapA = processorA.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMapA.get("storage_start").get.asInstanceOf[Long] should be > 0L
    storageMapA.get("storage_count").get shouldBe 4L
    var streamMapA = processorA.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapA.get("stream_start").get.asInstanceOf[Long] should be > 0L
    streamMapA.get("stream_count").get.asInstanceOf[Long] shouldBe 2L
    processorA.getStatus.get("process_timestamp").get shouldBe "2015-11-11T11:11:12"

    processorB.getStatus.get("process_phase").get shouldBe "Stop"
    var storageMapB = processorB.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMapB.get("storage_start").get.asInstanceOf[Long] shouldBe 0L
    storageMapB.get("storage_count").get shouldBe 0L
    var streamMapB = processorB.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapB.get("stream_start").get.asInstanceOf[Long] shouldBe 0L
    streamMapB.get("stream_count").get.asInstanceOf[Long] shouldBe 0L
    processorB.getStatus.get("process_timestamp").get shouldBe ""

    val (staticInfoA, streamInfoA) = stopFunA()

    val stopFunB = processorB.startJValueProcessing(rdd => rdd.count)._1
    while (processorB.phase == StoragePhase) {
      Thread.sleep(1000)
    }
    Thread.sleep(10000)
    processorA.getStatus.get("process_phase").get shouldBe "Stop"
    storageMapA = processorA.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMapA.get("storage_start").get.asInstanceOf[Long] should be > 0L
    storageMapA.get("storage_count").get shouldBe 4L
    streamMapA = processorA.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapA.get("stream_start").get.asInstanceOf[Long] should be > 0L
    streamMapA.get("stream_count").get.asInstanceOf[Long] shouldBe 2L
    processorA.getStatus.get("process_timestamp").get shouldBe "2015-11-11T11:11:12"

    processorB.getStatus.get("process_phase").get shouldBe "Stream"
    storageMapB = processorB.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMapB.get("storage_start").get.asInstanceOf[Long] should be > 0L
    storageMapB.get("storage_count").get shouldBe 2L
    streamMapB = processorB.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapB.get("stream_start").get.asInstanceOf[Long] should be > 0L
    streamMapB.get("stream_count").get.asInstanceOf[Long] shouldBe 1L
    processorB.getStatus.get("process_timestamp").get shouldBe "2016-11-11T11:11:13"

    val (staticInfoB, streamInfoB) = stopFunB()

    processorA.getStatus.get("process_phase").get shouldBe "Stop"
    processorB.getStatus.get("process_phase").get shouldBe "Stop"

    //
    storageMapA = processorA.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    storageMapB = processorB.getStatus.get("storage").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapA = processorA.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]
    streamMapB = processorB.getStatus.get("stream").get.asInstanceOf[LinkedHashMap[String,Any]]

    storageMapA.get("storage_start").get.asInstanceOf[Long] should not be storageMapB.get("storage_start").get.asInstanceOf[Long]
    streamMapA.get("stream_start").get.asInstanceOf[Long] should not be streamMapB.get("stream_start").get.asInstanceOf[Long]
  }

  override def afterAll = {
    sc.stop()
  }

  def sendKafkaMessage(address:String, topic: String, message: Array[String]):Unit = {
    val prop = new Properties()
    prop.put("metadata.broker.list", address)
    prop.put("serializer.class","kafka.serializer.StringEncoder")
    val producerConfig = new ProducerConfig(prop)
    val producer = new Producer[String, String](producerConfig)
    message.foreach({ line =>
      val message = new KeyedMessage[String, String](topic, line)
      producer.send(message)
    })
    producer.close()
  }
}

class SQLSpec
  extends FeatureSpec
  with GivenWhenThen
  with ShouldMatchers
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "SQLSpec")
  val sqlc = new SQLContext(sc)

  import sqlc._

  val dummyDataUrl = "file://src/test/resources/dummydata"

  feature("The user can query stored JSON with SQL via an inferred schema") {
    scenario("The inferred schema is used correctly") {
      val startTime = System.nanoTime()
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("no schema is provided by the user")
      val schema: Option[StructType] = None

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      val maxIdFun = processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, gender FROM test")
        resultData ++= resultRdd.collect()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished
      val executionTime = System.nanoTime() - startTime
      executionTime.toDouble should be < 10e9 // less than 10 seconds

      Then("all rows should be processed")
      val maxId = maxIdFun()
      // compare to the contents of src/test/resources/3.json
      maxId shouldBe Some("2014-11-21T14:54:27")
      resultData should contain theSameElementsAs
        List(Row(21, "m"), Row(22, "f"), Row(23, "f"), Row(24, "f"),
        Row(21, "m"), Row(18, "f"), Row(22, "m"), Row(31, "f"), Row(23, "m"),
          Row(19, "m"), Row(24, "m"), Row(26, "f"))
    }

    scenario("The inferred schema is used badly") {
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("no schema is provided by the user")
      val schema: Option[StructType] = None

      And("a statement with bad columns is run")
      processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        sql("SELECT name, gender FROM test").count
      }, schema)
      processor.state shouldBe Running

      Then("an exception should be thrown during processing")
      val thrown = the[TreeNodeException[_]] thrownBy processor.awaitTermination()
      thrown.getMessage should startWith("Unresolved attributes: 'name")
      processor.state shouldBe Finished
    }

    scenario("An unknown table name is used") {
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("no schema is provided by the user")
      val schema: Option[StructType] = None

      And("a statement with bad table name is run")
      processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        sql("SELECT name, gender FROM test2").count
      }, schema)
      processor.state shouldBe Running

      Then("an exception should be thrown during processing")
      val thrown = the[RuntimeException] thrownBy processor.awaitTermination()
      thrown.getMessage should startWith("Table Not Found: test2")
      processor.state shouldBe Finished
    }

    scenario("An empty data set is used") {
      Given("an empty data set")
      val processor = new HybridProcessor(sc, sqlc, "empty", Nil)
      processor.state shouldBe Initialized

      When("no schema is provided by the user")
      val schema: Option[StructType] = None

      And("any statement is run")
      var numberOfCalls = 0
      var numberOfItems = 0L
      processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, gender FROM test")
        numberOfCalls += 1
        numberOfItems += resultRdd.count()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished

      Then("no rows should be processed")
      numberOfItems shouldBe 0
      And("the function should not ever be called")
      numberOfCalls shouldBe 0
    }
  }

  feature("The user can query stored JSON with SQL via a given schema") {
    scenario("The given schema is complete and correct") {
      val startTime = System.nanoTime()
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("the correct schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false),
        StructField("gender", StringType, nullable = false))))

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      val maxIdFun = processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, gender FROM test")
        resultData ++= resultRdd.collect()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished
      val executionTime = System.nanoTime() - startTime
      executionTime.toDouble should be < 10e9 // less than 10 seconds

      Then("all rows should be processed")
      val maxId = maxIdFun()
      // compare to the contents of src/test/resources/3.json
      maxId shouldBe Some("2014-11-21T14:54:27")
      resultData should contain theSameElementsAs
        List(Row(21, "m"), Row(22, "f"), Row(23, "f"), Row(24, "f"),
          Row(21, "m"), Row(18, "f"), Row(22, "m"), Row(31, "f"), Row(23, "m"),
          Row(19, "m"), Row(24, "m"), Row(26, "f"))
    }

    scenario("The given schema covers only a subset of columns") {
      val startTime = System.nanoTime()
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("an incomplete schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false))))

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      val maxIdFun = processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age FROM test")
        resultData ++= resultRdd.collect()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished
      val executionTime = System.nanoTime() - startTime
      executionTime.toDouble should be < 10e9 // less than 10 seconds

      Then("all rows should be processed")
      val maxId = maxIdFun()
      // compare to the contents of src/test/resources/3.json
      maxId shouldBe Some("2014-11-21T14:54:27")
      resultData should contain theSameElementsAs
        List(Row(21), Row(22), Row(23), Row(24),
          Row(21), Row(18), Row(22), Row(31), Row(23),
          Row(19), Row(24), Row(26))
    }

    scenario("The given schema contains a wrong datatype") {
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("a wrong schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false),
        StructField("gender", IntegerType, nullable = false) // actually: StringType
      )))

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        sql("SELECT age, gender FROM test").count
      }, schema)
      processor.state shouldBe Running

      Then("an exception should be thrown during processing")
      val thrown = the[SparkException] thrownBy processor.awaitTermination()
      thrown.getMessage should include("ClassCastException: java.lang.String" +
        " cannot be cast to java.lang.Integer")
      processor.state shouldBe Finished
    }

    scenario("The given schema includes additional nullable columns") {
      val startTime = System.nanoTime()
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("an extended schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true))))

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      val maxIdFun = processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, name FROM test")
        resultData ++= resultRdd.collect()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished
      val executionTime = System.nanoTime() - startTime
      executionTime.toDouble should be < 10e9 // less than 10 seconds

      Then("missing values should be nulled")
      val maxId = maxIdFun()
      // compare to the contents of src/test/resources/3.json
      maxId shouldBe Some("2014-11-21T14:54:27")
      resultData should contain theSameElementsAs
        List(Row(21, null), Row(22, null), Row(23, null), Row(24, null),
          Row(21, null), Row(18, null), Row(22, null), Row(31, null), Row(23, null),
          Row(19, null), Row(24, null), Row(26, null))
    }

    scenario("The given schema includes additional non-nullable columns") {
      val startTime = System.nanoTime()
      Given("a test data set")
      val processor = new HybridProcessor(sc, sqlc, dummyDataUrl, Nil)
      processor.state shouldBe Initialized

      When("an extended schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false))))

      And("a correct statement is run")
      var resultData: Array[Row] = Array()
      val maxIdFun = processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, name FROM test")
        resultData ++= resultRdd.collect()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished
      val executionTime = System.nanoTime() - startTime
      executionTime.toDouble should be < 10e9 // less than 10 seconds

      Then("missing values should be nulled")
      // TODO: actually they should not (Spark issue?)
      val maxId = maxIdFun()
      // compare to the contents of src/test/resources/3.json
      maxId shouldBe Some("2014-11-21T14:54:27")
      resultData should contain theSameElementsAs
        List(Row(21, null), Row(22, null), Row(23, null), Row(24, null),
          Row(21, null), Row(18, null), Row(22, null), Row(31, null), Row(23, null),
          Row(19, null), Row(24, null), Row(26, null))
    }

    scenario("An empty data set is used") {
      Given("an empty data set")
      val processor = new HybridProcessor(sc, sqlc, "empty", Nil)
      processor.state shouldBe Initialized

      When("an extended schema is provided by the user")
      val schema: Option[StructType] = Some(StructType(List(
        StructField("age", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false))))

      And("any statement is run")
      var numberOfCalls = 0
      var numberOfItems = 0L
      processor.startTableProcessing(rdd => {
        rdd.registerTempTable("test")
        val resultRdd = sql("SELECT age, gender FROM test")
        numberOfCalls += 1
        numberOfItems += resultRdd.count()
      }, schema)._2
      processor.state shouldBe Running
      processor.awaitTermination()
      processor.state shouldBe Finished

      Then("no rows should be processed")
      numberOfItems shouldBe 0
      And("the function should not ever be called")
      numberOfCalls shouldBe 0
    }
  }

  override def afterAll = {
    sc.stop()
  }
}
