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

import java.io.{FileNotFoundException, FileInputStream}
import java.util.Properties

import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.{StringType, LongType, StructField, StructType}

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
    processor.start(rdd => rdd)
    processor.awaitTermination()
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 5e9 // less than 5 seconds
  }

  "Static-only processing (local files)" should "end when the processing is done" taggedAs (LocalTest) in {
    val startTime = System.nanoTime()
    val processor = new HybridProcessor(sc, sqlc, "file://src/test/resources/dummydata", Nil)
    processor.start(rdd => rdd)
    processor.awaitTermination()
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e9 // less than 10 seconds
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
    val startTime = System.nanoTime()
    val stopFun = processor.start(rdd => rdd)._1

    processor.awaitTermination()
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
    val startTime = System.nanoTime()
    val stopFun = processor.start(rdd => rdd)._1
    Thread.sleep(5000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    val executionTime = (System.nanoTime() - startTime)
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
    val stopFun = processor.start(rdd => rdd)._1
    processor.awaitTermination()
    val (staticInfo, streamInfo) = stopFun()
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime shouldBe 0L
    streamInfo.maxId shouldBe empty
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
    val startTime = System.nanoTime()
    val stopFun = processor.start(rdd => rdd)._1
    Thread.sleep(1700) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
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
    val startTime = System.nanoTime()
    val stopFun = processor.start(rdd => rdd)._1
    Thread.sleep(10000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 10e10 // less than 100 seconds
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount should be > 10000L
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
  }

  it should "be processable using SQL" taggedAs (KafkaTest) in {
    val path = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    val startTime = System.nanoTime()
    val schema = StructType(List(StructField("video_id", LongType, false),
      StructField("title", StringType, false)))
    import sqlc._
    val stopFun = processor.start(rdd => {
      rdd.registerTempTable("test")
      sql("SELECT video_id FROM test LIMIT 10")
    }, Some(schema))._1
    Thread.sleep(15000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    val executionTime = (System.nanoTime() - startTime)
    executionTime.toDouble should be < 40e9 // less than 40 seconds
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount should be > 0L
    streamInfo.itemCount should be < 100L
    streamInfo.runtime should be > 0L
    streamInfo.maxId should not be empty
  }

  "Kafka-only processing on an empty topic" should "not process anything" taggedAs (KafkaTest) in {
    Thread.sleep(2000)
    val path = s"kafka://$kafkaPath/notopic/1"
    val processor = new HybridProcessor(sc, sqlc, "empty", path :: Nil)
    val stopFun = processor.start(rdd => rdd)._1
    Thread.sleep(10000) // if we stop during the first batch, something goes wrong
    val (staticInfo, streamInfo) = stopFun()
    // check number of items and received id
    staticInfo.itemCount shouldBe 0L
    staticInfo.runtime should be > 0L
    staticInfo.maxId shouldBe empty
    streamInfo.itemCount shouldBe 0L
    streamInfo.runtime should be > 0L
    streamInfo.maxId shouldBe empty
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
  val sc = new SparkContext("local[3]", "KafkaStreamSpec")
  val sqlc = new SQLContext(sc)

  "HDFS+Kafka processing" should "change processing smoothly" taggedAs (HDFSTest, KafkaTest) in {
    val hdfsPath = "hdfs:///user/fluentd/dummy"
    val kafkaURI = s"kafka://$kafkaPath/dummy/1"
    val processor = new HybridProcessor(sc, sqlc, hdfsPath, kafkaURI :: Nil)
    val stopFun = processor.start(rdd => rdd)._1
    Thread.sleep(60000)
    val (staticInfo, streamInfo) = stopFun()
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
