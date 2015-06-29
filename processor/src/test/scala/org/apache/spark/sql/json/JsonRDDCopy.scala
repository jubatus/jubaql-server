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
//
// This file is based on sql/core/src/main/scala/org/apache/spark/sql/json/JsonRDD.scala
// from Apache Spark 1.1.1 and incorporates code covered by the following terms:
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE_SPARK file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.spark.sql.json

import scala.collection.Map
import scala.collection.convert.Wrappers.{JMapWrapper, JListWrapper}
import scala.math.BigDecimal

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.Logging

// The purpose of this copy is to make some private functions
// publicly available for testing.
object JsonRDDCopy extends Logging {

  private[sql] def jsonStringToRow(
                                    json: RDD[String],
                                    schema: StructType): RDD[Row] = {
    parseJson(json).map(parsed => asRow(parsed, schema))
  }

  private[sql] def inferSchema(
                                json: RDD[String],
                                samplingRatio: Double = 1.0): StructType = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) json else json.sample(false, samplingRatio, 1)
    val allKeys = parseJson(schemaData).map(allKeysWithValueTypes).reduce(_ ++ _)
    createSchema(allKeys)
  }

  /* was: private */ def createSchema(allKeys: Set[(String, DataType)]): StructType = {
    // Resolve type conflicts
    val resolved = allKeys.groupBy {
      case (key, dataType) => key
    }.map {
      // Now, keys and types are organized in the format of
      // key -> Set(type1, type2, ...).
      case (key, typeSet) => {
        val fieldName = key.substring(1, key.length - 1).split("`.`").toSeq
        val dataType = typeSet.map {
          case (_, dataType) => dataType
        }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

        (fieldName, dataType)
      }
    }

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (topLevel, structLike) = values.partition(_.size == 1)
      val topLevelFields = topLevel.filter {
        name => resolved.get(prefix ++ name).get match {
          case ArrayType(StructType(Nil), _) => false
          case ArrayType(_, _) => true
          case struct: StructType => false
          case _ => true
        }
      }.map {
        a => StructField(a.head, resolved.get(prefix ++ a).get, nullable = true)
      }

      val structFields: Seq[StructField] = structLike.groupBy(_(0)).map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          val dataType = resolved.get(prefix :+ name).get
          dataType match {
            case array: ArrayType =>
              Some(StructField(name, ArrayType(structType, array.containsNull), nullable = true))
            case struct: StructType => Some(StructField(name, structType, nullable = true))
            // dataType is StringType means that we have resolved type conflicts involving
            // primitive types and complex types. So, the type of name has been relaxed to
            // StringType. Also, this field should have already been put in topLevelFields.
            case StringType => None
          }
        }
      }.flatMap(field => field).toSeq

      StructType(
        (topLevelFields ++ structFields).sortBy {
          case StructField(name, _, _, _) => name
        })
    }

    makeStruct(resolved.keySet.toSeq, Nil)
  }

  private[sql] def nullTypeToStringType(struct: StructType): StructType = {
    val fields = struct.fields.map {
      case StructField(fieldName, dataType, nullable, metadata) => {
        val newType = dataType match {
          case NullType => StringType
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case ArrayType(struct: StructType, containsNull) =>
            ArrayType(nullTypeToStringType(struct), containsNull)
          case struct: StructType =>nullTypeToStringType(struct)
          case other: DataType => other
        }
        StructField(fieldName, newType, nullable, metadata)
      }
    }

    StructType(fields)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private[json] def compatibleType(t1: DataType, t2: DataType): DataType = {
    val commonType = HiveTypeCoercion.findTightestCommonType(t1, t2)
    if (commonType.isDefined) {
      commonType.get
    } else {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (other: DataType, NullType) => other
        case (NullType, other: DataType) => other
        case (StructType(fields1), StructType(fields2)) => {
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) => {
              val dataType = fieldTypes.map(field => field.dataType).reduce(
                (type1: DataType, type2: DataType) => compatibleType(type1, type2))
              StructField(name, dataType, true)
            }
          }
          StructType(newFields.toSeq.sortBy {
            case StructField(name, _, _, _) => name
          })
        }
        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
        // TODO: We should use JsonObjectStringType to mark that values of field will be
        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }

  private def typeOfPrimitiveValue: PartialFunction[Any, DataType] = {
    ScalaReflection.typeOfObject orElse {
      // Since we do not have a data type backed by BigInteger,
      // when we see a Java BigInteger, we use DecimalType.
      case value: java.math.BigInteger => DecimalType.Unlimited
      // DecimalType's JVMType is scala BigDecimal.
      case value: java.math.BigDecimal => DecimalType.Unlimited
      // Unexpected data type.
      case _ => StringType
    }
  }

  /**
   * Returns the element type of an JSON array. We go through all elements of this array
   * to detect any possible type conflict. We use [[compatibleType]] to resolve
   * type conflicts. Right now, when the element of an array is another array, we
   * treat the element as String.
   */
  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.exists(v => v == null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements.map {
        e => e match {
          case map: Map[_, _] => StructType(Nil)
          // We have an array of arrays. If those element arrays do not have the same
          // element types, we will return ArrayType[StringType].
          case seq: Seq[_] =>  typeOfArray(seq)
          case value => typeOfPrimitiveValue(value)
        }
      }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

      ArrayType(elementType, containsNull)
    }
  }

  /**
   * Figures out all key names and data types of values from a parsed JSON object
   * (in the format of Map[Stirng, Any]). When the value of a key is an JSON object, we
   * only use a placeholder (StructType(Nil)) to mark that it should be a struct
   * instead of getting all fields of this struct because a field does not appear
   * in this JSON object can appear in other JSON objects.
   */
  /* was: private */ def allKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    val keyValuePairs = m.map {
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, value) => (s"`$key`", value)
    }.toSet
    keyValuePairs.flatMap {
      case (key: String, struct: Map[_, _]) => {
        // The value associated with the key is an JSON object.
        allKeysWithValueTypes(struct.asInstanceOf[Map[String, Any]]).map {
          case (k, dataType) => (s"$key.$k", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: Seq[_]) => {
        // The value associated with the key is an array.
        typeOfArray(array) match {
          case ArrayType(StructType(Nil), containsNull) => {
            // The elements of this arrays are structs.
            array.asInstanceOf[Seq[Map[String, Any]]].flatMap {
              element => allKeysWithValueTypes(element)
            }.map {
              case (k, dataType) => (s"$key.$k", dataType)
            } :+ (key, ArrayType(StructType(Nil), containsNull))
          }
          case ArrayType(elementType, containsNull) =>
            (key, ArrayType(elementType, containsNull)) :: Nil
        }
      }
      case (key: String, value) => (key, typeOfPrimitiveValue(value)) :: Nil
    }
  }

  /**
   * Converts a Java Map/List to a Scala Map/Seq.
   * We do not use Jackson's scala module at here because
   * DefaultScalaModule in jackson-module-scala will make
   * the parsing very slow.
   */
  private def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[_, _] =>
      // .map(identity) is used as a workaround of non-serializable Map
      // generated by .mapValues.
      // This issue is documented at https://issues.scala-lang.org/browse/SI-7005
      JMapWrapper(map).mapValues(scalafy).map(identity)
    case list: java.util.List[_] =>
      JListWrapper(list).map(scalafy)
    case atom => atom
  }

  private def parseJson(json: RDD[String]): RDD[Map[String, Any]] = {
    // According to [Jackson-72: https://jira.codehaus.org/browse/JACKSON-72],
    // ObjectMapper will not return BigDecimal when
    // "DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS" is disabled
    // (see NumberDeserializer.deserialize for the logic).
    // But, we do not want to enable this feature because it will use BigDecimal
    // for every float number, which will be slow.
    // So, right now, we will have Infinity for those BigDecimal number.
    // TODO: Support BigDecimal.
    json.mapPartitions(iter => {
      // When there is a key appearing multiple times (a duplicate key),
      // the ObjectMapper will take the last value associated with this duplicate key.
      // For example: for {"key": 1, "key":2}, we will get "key"->2.
      val mapper = new ObjectMapper()
      iter.map { record =>
        val parsed = scalafy(mapper.readValue(record, classOf[java.util.Map[String, Any]]))
        parsed.asInstanceOf[Map[String, Any]]
      }
    })
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  private def toDecimal(value: Any): BigDecimal = {
    value match {
      case value: java.lang.Integer => BigDecimal(value)
      case value: java.lang.Long => BigDecimal(value)
      case value: java.math.BigInteger => BigDecimal(value)
      case value: java.lang.Double => BigDecimal(value)
      case value: java.math.BigDecimal => BigDecimal(value)
    }
  }

  private def toJsonArrayString(seq: Seq[Any]): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any]): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(s"""\"${key}\":${toString(value)}""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]])
      case value: Seq[_] => toJsonArrayString(value)
      case value => Option(value).map(_.toString).orNull
    }
  }

  private[json] def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case ArrayType(elementType, _) =>
          value.asInstanceOf[Seq[Any]].map(enforceCorrectType(_, elementType))
        case StringType => toString(value)
        case IntegerType => value.asInstanceOf[IntegerType.JvmType]
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case DecimalType() => toDecimal(value)
        case BooleanType => value.asInstanceOf[BooleanType.JvmType]
        case NullType => null
      }
    }
  }

  /* was: private */ def asRow(json: Map[String,Any], schema: StructType): Row = {
    // TODO: Reuse the row instead of creating a new one for every record.
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      // StructType
      case (StructField(name, fields: StructType, _, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          v => asRow(v.asInstanceOf[Map[String, Any]], fields)).orNull)

      // ArrayType(StructType)
      case (StructField(name, ArrayType(structType: StructType, _), _, _), i) =>
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[Any]].map(
              e => asRow(e.asInstanceOf[Map[String, Any]], structType))).orNull)

      // Other cases
      case (StructField(name, dataType, _, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          enforceCorrectType(_, dataType)).getOrElse(null))
    }

    row
  }
}