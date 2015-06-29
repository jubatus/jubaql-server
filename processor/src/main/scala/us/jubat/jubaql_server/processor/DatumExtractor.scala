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

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.sql.{ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, DataType, Row}
import org.json4s.JsonAST.JNumber
import org.json4s.native.JsonMethods
import org.json4s._
import us.jubat.common.Datum

import scala.collection.mutable
import scala.collection.concurrent
import scala.collection.JavaConversions._

object DatumExtractor {
  def extract(cm: CreateModel,
              data: String,
              featureFunctions: concurrent.Map[String, String],
              logger: Logger): Datum
    = extract(cm, JsonMethods.parse(data), featureFunctions, logger)

  def extract(cm: CreateModel,
              data: JValue,
              featureFunctions: concurrent.Map[String, String],
              logger: Logger): Datum = {
    // we can only process numeric and string values
    val filtered: List[(String, JValue)] = data.filterField {
      case JField(_, _: JNumber) | JField(_, _: JString) => true
      case _ => false
    }

    // fill the row with Spark-compatible values (String, Int, Long, Double)
    val row = Row(filtered.map {
      case (_, value: JString) =>
        value.s
      case (_, value: JInt) if value.num.isValidInt =>
          value.num.toInt
      case (_, value: JInt) =>
          // note: this may still overflow
          value.num.toLong
      case (_, value: JDecimal) =>
        value.num.toDouble
      case (_, value: JDouble) =>
        value.num
    }: _*)
    // set the correct type information for Spark
    val schema: Map[String, (Int, DataType)] = filtered.zipWithIndex.map {
        case ((key, _: JString), ix) =>
          (key, (ix, StringType))
        case ((key, value: JInt), ix) if value.num.isValidInt =>
            (key, (ix, IntegerType))
        case ((key, _: JInt), ix) =>
            (key, (ix, LongType))
        case ((key, _: JNumber), ix) =>
          (key, (ix, DoubleType))
      }.toMap

    extract(cm, schema, row, featureFunctions, logger)
  }

  def extract(cm: CreateModel,
              schema: Map[String, (Int, DataType)],
              row: Row,
              featureFunctions: concurrent.Map[String, String],
              logger: Logger): Datum = {
    // schema is a mapping (column name -> (column index, column type))

    val datum = new Datum()
    val ds = new DatumSetter(datum, row, logger)

    // schemaCopy holds current candidate columns for feature extraction.
    // one matches, then deleted from schemaCopy.
    val schemaCopy = mutable.Map[String, (Int, DataType)](schema.toSeq: _*)
    // remove a label column or a id column
    cm.labelOrId.foreach(schemaCopy -= _._2)

    def getFeatureFunctionBodyByName(f: String): String = {
      featureFunctions.get(f) match {
        case None =>
          val knownFuncs = featureFunctions.keys.mkString(", ")
          val msg = s"feature function '$f' is not found (known: ${knownFuncs})"
          logger.error(msg)
          throw new RuntimeException(msg)
        case Some(funcBody) =>
          funcBody
      }
    }

    // register feature functions
    cm.featureExtraction.foreach {
      case (_, "id") | (_, "unigram") | (_, "bigram") =>
        // do nothing

      case (NormalParameters(params), funcName) =>
        val funcBody = getFeatureFunctionBodyByName(funcName)
        JavaScriptFeatureFunctionManager.register(funcName, params.length, funcBody)

      case (_, funcName) =>
        val funcBody = getFeatureFunctionBodyByName(funcName)
        JavaScriptFeatureFunctionManager.register(funcName, 1, funcBody)
    }

    type SchemaType = Seq[(String, (Int, DataType))]

    def processWithoutFeatureFunction(column: (String, (Int, DataType))): Unit = column match {
      case (colName, (rowIdx, dataType)) =>
        ds.setFromRow(colName, rowIdx, dataType)
        schemaCopy -= colName
    }

    def processWithJubatusFeatureFunction(funcName: String, column: (String, (Int, DataType))): Unit = column match {
      case (colName, (rowIdx, dataType)) =>
        ds.setFromRow(s"$colName-$funcName-jubaconv", rowIdx, dataType)
        schemaCopy -= colName
    }

    def processWithFeatureFunction(columns: SchemaType, funcName: String): Unit = {
      val args: Seq[AnyRef] = columns.map {
        case (colName, (rowIdx, dataType)) =>
          ds.getFromRow(rowIdx, dataType) match {
            case None =>
              // TODO: improve message
              val msg = s"failed to get $colName"
              logger.error(msg)
              throw new RuntimeException(msg)

            case Some(arg) =>
              arg.asInstanceOf[AnyRef]
          }
      }

      val values = JavaScriptFeatureFunctionManager.callAndGetValues(funcName, args: _*)

      val catArgNames = columns.map(_._1).mkString(",")
      val outputColNameCommon = s"$funcName#$catArgNames"

      values.foreach {
        case (key, value) =>
          val outputColName =
            // if we have a single-valued return function, omit the object key,
            // otherwise add it to the datum's key string
            if (values.size == 1) {
              outputColNameCommon
            } else {
              outputColNameCommon + "#" + key
            }
          value match {
            case s: String =>
              ds.set(outputColName, s)
            case x: Double =>
              ds.set(outputColName, x)
          }
      }
      columns.foreach(schemaCopy -= _._1)
    }

    def processColumns(funcName: String, columns: Seq[(String, (Int, DataType))]) = {
      funcName match {
        case "id" =>
          columns.foreach(processWithoutFeatureFunction)
        case "unigram" | "bigram" =>
          columns.foreach(processWithJubatusFeatureFunction(funcName, _))
        case _ =>
          columns.foreach {
            case arg =>
              processWithFeatureFunction(Seq(arg), funcName)
          }
      }
    }

    cm.featureExtraction.foreach {
      // *
      case (WildcardAnyParameter, funcName) =>
        processColumns(funcName, schemaCopy.toSeq)

      // prefix_*
      case (WildcardWithPrefixParameter(prefix), funcName) =>
        val processedColumns = schemaCopy.filter(_._1.startsWith(prefix)).toSeq
        processColumns(funcName, processedColumns)

      // *_suffix
      case (WildcardWithSuffixParameter(suffix), funcName) =>
        val processedColumns = schemaCopy.filter(_._1.endsWith(suffix)).toSeq
        processColumns(funcName, processedColumns)

      // not wildcard
      case (NormalParameters(params), funcName) =>
        params match {
          case Nil =>
            val msg = "should not pass here. (this may be a bug of parser)"
            logger.error(msg)
            throw new RuntimeException(msg) // maybe RuntimeException is inappropriate...

          case colNames =>
            val columns: SchemaType = params.map {
              case colName =>
                // if we have an explicitly specified column name,
                // then it does not matter whether it was used before or
                // not, so we access `schema`, not `schemaCopy`
                schema.get(colName) match {
                  case None =>
                    val msg = s"column named '$colName' not found"
                    logger.error(msg)
                    throw new RuntimeException(msg)
                  case Some((rowIdx, dataType)) =>
                    (colName, (rowIdx, dataType))
                }
            }

            funcName match {
              case "id" if colNames.length == 1 =>
                processWithoutFeatureFunction(columns.head)
              case "unigram" | "bigram" if colNames.length == 1 =>
                processWithJubatusFeatureFunction(funcName, columns.head)
              case "id" =>
                val msg = "attempt to call id feature function with more than one arguments"
                logger.error(msg)
                throw new RuntimeException(msg)
              case "unigram" | "bigram" =>
                val msg = "attempt to call Jubatus feature function with more than one argument"
                logger.error(msg)
                throw new RuntimeException(msg)
              case _ =>
                processWithFeatureFunction(columns, funcName)
            }
        }
    }

    datum
  }
}

private class DatumSetter(d: Datum, row: Row, logger: Logger) {
  def getFromRow(rowIdx: Int, dataType: DataType): Option[Any] = dataType match {
    case _ if row.isNullAt(rowIdx) =>
      // a null value is a property of one particular data value, so
      // we will just ignore this value and continue with the next
      None
    case StringType =>
      Some(row.getString(rowIdx))
    case FloatType =>
      Some(row.getFloat(rowIdx).toDouble)
    case DoubleType =>
      Some(row.getDouble(rowIdx))
    case ShortType =>
      Some(row.getShort(rowIdx).toDouble)
    case IntegerType =>
      Some(row.getInt(rowIdx).toDouble)
    case LongType =>
      Some(row.getLong(rowIdx).toDouble)
    case other =>
      logger.warn(s"cannot take value of type '$other' from row $row")
      None
  }
  def setFromRow(colName: String, rowIdx: Int, dataType: DataType) = {
    getFromRow(rowIdx, dataType) match {
      case Some(s: String) =>
        set(colName, s)
      case Some(x: Double) =>
        set(colName, x)
      case _ =>
        // do nothing
    }
  }

  def set(colName: String, value: String) = d.addString(colName, value)
  def set(colName: String, value: Double) = d.addNumber(colName, value)
}
