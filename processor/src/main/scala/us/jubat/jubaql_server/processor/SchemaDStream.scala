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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{StreamingContext, Time}

case class SchemaDStream(sqlc: SQLContext,
                         dataStream: DStream[Row],
                         schemaStream: DStream[StructType]) {
  def print() = foreachRDD(rdd => {
    println(rdd.schema)
    rdd.foreach(println)
  })

  def registerStreamAsTable(name: String): Unit = {
    foreachRDD(_.registerTempTable(name))
  }

  def foreachRDD(func: SchemaRDD => Unit): Unit = {
    // We want to simulate stream.foreachRDD on the dataStream,
    // but if we say dataStream.foreachRDD(...), we don't have
    // access to the schema. the only way to merge the two
    // streams is dataStream.transformWith(schemaStream ...).
    // Therefore we use this transformWith() function, apply
    // the function obtained as a parameter therein, and call
    // count() to force execution.
    def executeFunction(dataRDD: RDD[Row], schemaRDD: RDD[StructType]): RDD[Unit] = {
      val schema: StructType = schemaRDD.collect.head
      val dataWithSchema: SchemaRDD = sqlc.applySchema(dataRDD, schema)
      val result = func(dataWithSchema)
      schemaRDD.map(x => result)
    }
    dataStream.transformWith(schemaStream, executeFunction _).foreachRDD(_.count())
  }
}

/**
 * Helper object to construct a SchemaDStream from various input formats.
 */
object SchemaDStream {

  private class RegisteredTableDStream(@transient ssc: StreamingContext,
                                       @transient sqlc: SQLContext,
                                       tableName: String) extends InputDStream[Row](ssc) {
    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def compute(validTime: Time): Option[RDD[Row]] = {
      Some(sqlc.table(tableName))
    }
  }

  private class SQLResultDStream(@transient ssc: StreamingContext,
                                 @transient sqlc: SQLContext,
                                 stmt: Either[String, LogicalPlan]) extends InputDStream[Row](ssc) {
    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def compute(validTime: Time): Option[RDD[Row]] = {
      val rdd = stmt match {
        case Left(s) =>
          sqlc.sql(s)
        case Right(p) =>
          new SchemaRDD(sqlc, p)
      }
      Some(rdd)
    }
  }

  /**
   * Create a SchemaDStream from a DStream of JSON strings using schema inference.
   *
   * @param regName if Some(s), also register the created stream as a table with that name
   */
  def fromStringStream(sqlc: SQLContext,
                       stream: DStream[String],
                       regName: Option[String]): SchemaDStream = {
    val schemaStream: DStream[StructType] = stream.transform(rdd => {
      try {
        // the following call will compute the input RDD for schema
        // inference even if it is never used afterwards
        rdd.context.parallelize(sqlc.jsonRDD(rdd, 0.5).schema :: Nil)
      } catch {
        case e: UnsupportedOperationException if e.getMessage == "empty collection" =>
          // if the collection is empty, we cannot infer the schema, so we
          // return an empty schema.
          // NB. Executing SQL on this (empty) SchemaRDD will fail because the
          // columns are not known. It is the user's responsibility to
          // do the "right thing" in that case.
          val schema = StructType(Nil)
          rdd.context.parallelize(schema :: Nil)
        case e: Throwable =>
          throw e
      }
    })
    val rowStream: DStream[Row] = stream.transformWith(schemaStream,
      (rows: RDD[String], schemas: RDD[StructType]) => {
        val schema = schemas.collect().head
        val rdd = sqlc.jsonRDD(rows, schema)
        rdd
      }).cache() // This `cache()` is required for Spark 1.2.2.
    // register stream as a table
    val resultStream = SchemaDStream(sqlc, rowStream, schemaStream)
    regName.foreach(resultStream.registerStreamAsTable)
    resultStream
  }

  /**
   * Create a SchemaDStream from a DStream of JSON strings using given schema.
   *
   * @param regName if Some(s), also register the created stream as a table with that name
   */
  def fromStringStreamWithSchema(sqlc: SQLContext,
                                 stream: DStream[String],
                                 schema: StructType,
                                 regName: Option[String]): SchemaDStream = {
    val schemaStream: DStream[StructType] = stream.transform(rdd => {
      rdd.context.parallelize(schema :: Nil)
    })
    val rowStream: DStream[Row] = stream.transform((rows: RDD[String]) => {
      sqlc.jsonRDD(rows, schema)
    }).cache() // This `cache()` is required for Spark 1.2.2.
    // register stream as a table
    val resultStream = SchemaDStream(sqlc, rowStream, schemaStream)
    regName.foreach(resultStream.registerStreamAsTable)
    resultStream
  }

  /**
   * Create a SchemaDStream as a transformation on a previously registered stream.
   *
   * @param lookupName name of the stream to operate on, as per `registerStreamAsTable()`
   * @param transformation transformation of the stream
   * @param regName if Some(s), also register the created stream as a table with that name
   */
  def fromRDDTransformation(ssc: StreamingContext,
                            sqlc: SQLContext,
                            lookupName: String,
                            transformation: SchemaRDD => SchemaRDD,
                            regName: Option[String]): SchemaDStream = {
    val baseStream = new RegisteredTableDStream(ssc, sqlc,
      lookupName).cache() // This `cache()` is required for Spark 1.2.2.
    val schemaStream = baseStream.transform(rdd => rdd match {
      case s: SchemaRDD =>
        rdd.context.parallelize(transformation(s).schema :: Nil)
    })
    // NB. Just evolving schema and row stream independent of each other
    // does not seem to be enough any more in Spark 1.2. We also need
    // to call `sqlc.applySchema()` with the new schema or we will run
    // into a mysterious "is not cached" exception.
    val rowStream = baseStream.transformWith(schemaStream,
          (rdd: RDD[Row], schemaRDD: RDD[StructType]) => {
        val schema = schemaRDD.collect()(0)
        val outRdd = rdd match {
          case s: SchemaRDD =>
            transformation(s)
        }
        sqlc.applySchema(outRdd, schema)
      }).cache() // This `cache()` is required for Spark 1.2.2.
    // register stream as a table
    val resultStream = SchemaDStream(sqlc, rowStream, schemaStream)
    regName.foreach(resultStream.registerStreamAsTable)
    resultStream
  }

  /**
   * Create a SchemaDStream from a previously registered stream.
   *
   * @param lookupName name of the stream, as per `registerStreamAsTable()`
   * @return
   */
  def fromTableName(ssc: StreamingContext,
                    sqlc: SQLContext,
                    lookupName: String):
  SchemaDStream = {
    fromRDDTransformation(ssc, sqlc, lookupName, x => x, None)
  }

  /**
   * Create a SchemaDStream as result of an SQL query in each interval.
   *
   * @param statement SQL statement (used tables must have been registered before)
   * @param regName if Some(s), also register the created stream as a table with that name
   */
  def fromSQL(ssc: StreamingContext,
              sqlc: SQLContext,
              statement: String,
              regName: Option[String]): SchemaDStream = {
    val baseStream = new SQLResultDStream(ssc, sqlc,
      Left(statement)).cache() // This `cache()` is required for Spark 1.2.2.
    val schemaStream = baseStream.transform(rdd => rdd match {
      case s: SchemaRDD =>
        rdd.context.parallelize(s.schema :: Nil)
    })
    // register stream as a table
    val resultStream = SchemaDStream(sqlc, baseStream, schemaStream)
    regName.foreach(resultStream.registerStreamAsTable)
    resultStream
  }

  /**
   * Create a SchemaDStream as result of an SQL query in each interval.
   *
   * @param selectPlan SQL plan (used tables must have been registered before)
   * @param regName if Some(s), also register the created stream as a table with that name
   */
  def fromSQL(ssc: StreamingContext,
              sqlc: SQLContext,
              selectPlan: LogicalPlan,
              regName: Option[String]): SchemaDStream = {
    val baseStream = new SQLResultDStream(ssc, sqlc,
      Right(selectPlan)).cache() // This `cache()` is required for Spark 1.2.2.
    val schemaStream = baseStream.transform(rdd => rdd match {
      case s: SchemaRDD =>
        rdd.context.parallelize(s.schema :: Nil)
    })
    // register stream as a table
    val resultStream = SchemaDStream(sqlc, baseStream, schemaStream)
    regName.foreach(resultStream.registerStreamAsTable)
    resultStream
  }
}
