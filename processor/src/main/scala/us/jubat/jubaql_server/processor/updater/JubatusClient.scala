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
package us.jubat.jubaql_server.processor.updater

import scala.collection.{immutable, mutable}
import com.typesafe.scalalogging.slf4j.Logger
import us.jubat.jubaql_server.processor._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row, StructType}
import org.slf4j.LoggerFactory
import us.jubat.common.Datum

trait JubatusClient {
  /**
   * Update the model using the given data with the given schema.
   *
   * This method will iterate over the given data, taking column names
   * and types from the given schema. For each item in the iterator,
   * a datum will be extracted using the rules given when creating the
   * model and the model will be updated with an "appropriate" method.
   */
  def update(schema: StructType, iter: Iterator[Row], statusUrl: String): Unit = {
    // create a mapping (column name -> (column index, column type))
    val name2rowDesc = (schema.fields.zipWithIndex.map { case (field, i) => {
      (field.name, (i, field.dataType))
    }
    }).toMap
    // set up the status poll with the Spark driver
    HttpClientPerJvm.startChecking(statusUrl)
    update(name2rowDesc, iter)
  }

  protected def update(colNameToColIndexAndType: Map[String, (Int, DataType)],
                       iter: Iterator[Row]): Unit


  /**
   * Get a function to batch analyze a model using the given RPC method.
   *
   * The returned function will iterate over the given data, taking
   * column names and types from the given schema. Each item in the
   * iterator will be used as input to the given RPC method, using the
   * conversion rules given when creating the model (if applicable).
   *
   * @param rpcName
   * @return
   */
  def getAnalyzeMethod(rpcName: String):
  (DataType, (StructType, Iterator[Row], String) => Iterator[Row]) = {
    // get the actual method from a subclass
    val (returnType, actualMethod) = analyzeMethod(rpcName)
    (returnType, (schema, iter, statusUrl) => {
      // create a mapping (column name -> (column index, column type))
      val name2rowDesc = (schema.fields.zipWithIndex.map { case (field, i) => {
        (field.name, (i, field.dataType))
      }
      }).toMap
      // set up the status poll with the Spark driver
      HttpClientPerJvm.startChecking(statusUrl)
      actualMethod(name2rowDesc, iter)
    })
  }

  protected def analyzeMethod(rpcName: String):
  (DataType, (Map[String, (Int, DataType)], Iterator[Row]) => Iterator[Row])

  protected def createLogger: Logger = {
    Logger(LoggerFactory getLogger getClass.getName)
  }

  protected def extractIdOrLabel(colName: String,
                                 schema: Map[String, (Int, DataType)],
                                 row: Row,
                                 logger: Logger): String = {
    schema.get(colName) match {
      // if the schema does not contain one of the given columns, this
      // is an error independent of the current row and should not be ignored
      case None =>
        val msg = "the given schema %s does not contain a column named '%s'".format(
          schema, colName)
        logger.error(msg)
        throw new RuntimeException(msg)

      // otherwise, extract the value in that column and return as string
      case Some((rowIdx, dataType)) =>
        dataType match {
          case _ if row.isNullAt(rowIdx) =>
            // a null value is a property of one particular data value, so
            // we will just ignore this value and continue with the next
            logger.warn(s"row $row has a NULL value in column $colName")
            "NULL"
          case StringType =>
            row.getString(rowIdx)
          case DoubleType =>
            row.getDouble(rowIdx).toString
          case IntegerType =>
            row.getInt(rowIdx).toString
          case LongType =>
            row.getLong(rowIdx).toString
          case FloatType =>
            row.getFloat(rowIdx).toString
          case ShortType =>
            row.getShort(rowIdx).toString
          case other => // DecimalType, StructType, ...
            logger.warn(s"cannot add value of type '$other' in row $row to datum")
            row.getString(rowIdx)
        }
    }
  }

  protected def retry[T](maxRetry: Int, logger: => Logger)(func: => T): T = {
    try {
      func
    } catch {
      case e: Throwable if maxRetry > 0 =>
        logger.warn(s"caught $e, retrying max. $maxRetry times")
        retry(maxRetry-1, logger)(func)
    }
  }
}

/*
 * Actually, we would like to use the function
 *   iter => updater.apply(schema, iter, statusUrl),
 * but this seems to close over more than is visible from that line,
 * so that the function becomes unserializable, even though all components
 * that we use are serializable.
 *
 * Our workaround is to define the case class below, which does not require
 * a closure over local variables. (Actually, it is exactly the same
 * class that *should* be generated by the compiler.)
 */
case class UpdaterApplyWrapper(schema: StructType, statusUrl: String, client: JubatusClient) {
  def apply(iter: Iterator[Row]) = client.update(schema, iter, statusUrl)
}

case class UpdaterAnalyzeWrapper(schema: StructType, statusUrl: String, client: JubatusClient, rpcName: String) {
  val (dataType, analyzeMethod) = client.getAnalyzeMethod(rpcName)

  def apply(iter: Iterator[Row]) = analyzeMethod(schema, iter, statusUrl)
}
