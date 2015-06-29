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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

class SchemaDStreamSpec extends FlatSpec
with ShouldMatchers
with BeforeAndAfter
with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "SlidingWindow")
  val dummyData = sc.parallelize(
    """{"gender":"m","age":21,"jubaql_timestamp":"2014-11-21T15:52:21.943321112"}""" ::
      """{"gender":"f","age":22,"jubaql_timestamp":"2014-11-21T15:52:22"}""" ::
      """{"gender":"f","age":23,"jubaql_timestamp":"2014-11-21T15:52:23.123"}""" ::
      Nil)
  var ssc: StreamingContext = null
  var sqlc: SQLContext = null

  before {
    ssc = new StreamingContext(sc, Seconds(1))
    sqlc = new SQLContext(sc)
  }

  "fromStringStream" should "create a queryable stream" in {
    val rawStream: DStream[String] = new ConstantInputDStream(ssc, dummyData)
    SchemaDStream.fromStringStream(sqlc, rawStream, Some("hoge"))

    val collectedData = mutable.ListBuffer[Array[Row]]()
    rawStream.foreachRDD(_ => {
      val data = sqlc.sql("SELECT age FROM hoge").collect()
      collectedData += data
    })
    waitUntilProcessingEnds(rawStream, 1)

    collectedData.size should be > (0)
    val firstBatch = collectedData(0)
    firstBatch.size shouldBe 3
    firstBatch(0).getInt(0) shouldBe 21
    firstBatch(1).getInt(0) shouldBe 22
    firstBatch(2).getInt(0) shouldBe 23
  }

  "fromStringStreamWithSchema" should "create a queryable stream" in {
    val rawStream: DStream[String] = new ConstantInputDStream(ssc, dummyData)
    val schema = StructType(StructField("age", IntegerType, nullable = false) :: Nil)
    SchemaDStream.fromStringStreamWithSchema(sqlc, rawStream, schema, Some("hoge"))

    val collectedData = mutable.ListBuffer[Array[Row]]()
    rawStream.foreachRDD(_ => {
      val data = sqlc.sql("SELECT age FROM hoge").collect()
      collectedData += data
    })
    waitUntilProcessingEnds(rawStream, 1)

    collectedData.size should be > (0)
    val firstBatch = collectedData(0)
    firstBatch.size shouldBe 3
    firstBatch(0).getInt(0) shouldBe 21
    firstBatch(1).getInt(0) shouldBe 22
    firstBatch(2).getInt(0) shouldBe 23
  }

  "fromRDDTransformation" should "transform registered RDDs" in {
    val rawStream: DStream[String] = new ConstantInputDStream(ssc, dummyData)
    SchemaDStream.fromStringStream(sqlc, rawStream, Some("hoge"))

    val modified = SchemaDStream.fromRDDTransformation(ssc, sqlc, "hoge", rdd => {
      val newSchema = StructType(StructField("len", IntegerType, nullable = false) :: Nil)
      val newRdd = rdd.map(row => {
        Row(row.getString(2).size)
      })
      sqlc.applySchema(newRdd, newSchema)
    }, Some("foo"))

    val collectedData = mutable.ListBuffer[Array[Row]]()
    rawStream.foreachRDD(_ => {
      val data = sqlc.sql("SELECT len FROM foo").collect()
      collectedData += data
    })
    waitUntilProcessingEnds(rawStream, 1)

    collectedData.size should be > (0)
    val firstBatch = collectedData(0)
    firstBatch.size shouldBe 3
    firstBatch(0).getInt(0) shouldBe 29
    firstBatch(1).getInt(0) shouldBe 19
    firstBatch(2).getInt(0) shouldBe 23
  }

  "fromTableName" should "return registered streams" in {
    val rawStream: DStream[String] = new ConstantInputDStream(ssc, dummyData)
    SchemaDStream.fromStringStream(sqlc, rawStream, Some("hoge"))

    val stream = SchemaDStream.fromTableName(ssc, sqlc, "hoge")

    val collectedData = mutable.ListBuffer[Array[Row]]()
    stream.foreachRDD(rdd => {
      collectedData += rdd.collect()
    })
    waitUntilProcessingEnds(rawStream, 1)

    collectedData.size should be > (0)
    val firstBatch = collectedData(0)
    firstBatch.size shouldBe 3
    firstBatch(0).getInt(0) shouldBe 21
    firstBatch(1).getInt(0) shouldBe 22
    firstBatch(2).getInt(0) shouldBe 23
  }

  "fromSQL" should "find registered streams and register query outputs" in {
    val rawStream: DStream[String] = new ConstantInputDStream(ssc, dummyData)
    SchemaDStream.fromStringStream(sqlc, rawStream, Some("hoge"))

    SchemaDStream.fromSQL(ssc, sqlc,
      "SELECT jubaql_timestamp, age AS bar FROM hoge", Some("foo"))

    val collectedData = mutable.ListBuffer[Array[Row]]()
    rawStream.foreachRDD(_ => {
      val data = sqlc.sql("SELECT bar FROM foo").collect()
      collectedData += data
    })
    waitUntilProcessingEnds(rawStream, 1)

    collectedData.size should be > (0)
    val firstBatch = collectedData(0)
    firstBatch.size shouldBe 3
    firstBatch(0).getInt(0) shouldBe 21
    firstBatch(1).getInt(0) shouldBe 22
    firstBatch(2).getInt(0) shouldBe 23
  }

  override def afterAll(): Unit = {
    println("stopping SparkContext")
    sc.stop()
    super.afterAll()
  }

  protected def waitUntilProcessingEnds(stream: DStream[_], numIterations: Int) = {
    // count up in every interval
    val i = sc.accumulator(0)
    stream.foreachRDD(rdd => i += 1)
    // start processing
    ssc.start()
    // stop streaming context when i has become numIterations
    future {
      while (i.value < numIterations + 1)
        Thread.sleep(100)
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
    // wait for termination
    ssc.awaitTermination()
  }
}
