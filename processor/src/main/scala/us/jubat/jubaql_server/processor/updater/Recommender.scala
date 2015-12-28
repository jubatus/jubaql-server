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

import us.jubat.jubaql_server.processor.{CreateModel, DatumExtractor}
import us.jubat.jubaql_server.processor.json.DatumResult
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types._
import us.jubat.common.Datum
import us.jubat.recommender.RecommenderClient

import scala.collection.JavaConversions._
import scala.collection.concurrent

class Recommender(val jubaHost: String, jubaPort: Int, cm: CreateModel,
                  featureFunctions: concurrent.Map[String, String],
                  val idColumnName: String)
  extends JubatusClient with Serializable {

  // TODO wrap all network calls to Jubatus in try{} blocks

  override def update(rowSchema: Map[String, (Int, DataType)],
                      iter: Iterator[Row]): Unit = {
    // set up Jubatus client and logger
    val client = new RecommenderClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started RecommenderClient: $client")

    // get the index of the id column (if it exists)
    rowSchema.get(idColumnName) match {
      case None =>
        val msg = "the given schema %s does not contain a column named '%s'".format(
          rowSchema, idColumnName)
        logger.error(msg)
        throw new RuntimeException(msg)

      case Some((idIdx, _)) =>
        // loop over the data as long as the Spark driver is not stopped
        var stopped_? = HttpClientPerJvm.stopped
        if (stopped_?) {
          logger.debug("driver status is 'stopped', skip processing")
        }
        var batchStartTime = System.currentTimeMillis()
        var idx: Int = 0
        while (iter.hasNext && !stopped_?) {
          try {
            val row = iter.next
            if (!row.isNullAt(idIdx)) {
              // create datum and send to Jubatus
              val idValue = row.getString(idIdx)
              val datum = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
              retry(2, logger)(client.updateRow(idValue, datum))

              // every 1000 items, check if the Spark driver is still running
              if ((idx + 1) % 1000 == 0) {
                val duration = System.currentTimeMillis() - batchStartTime
                logger.debug(s"processed 1000 items using 'updateRow' method in $duration ms")
                stopped_? = HttpClientPerJvm.stopped
                if (stopped_?) {
                  logger.debug("driver status is 'stopped', end processing")
                }
                batchStartTime = System.currentTimeMillis()
              }
            } else {
              logger.warn("row %s has a NULL id".format(row))
            }
          } catch {
            case e: Exception =>
              logger.error(s"Failed to add row.", e)
          } finally {
            idx += 1
          }
        }
    }
  }

  override def analyzeMethod(rpcName: String) = {
    val returnType = StructType(
      StructField("string_values", MapType(StringType, StringType, valueContainsNull = false), nullable = true) ::
        StructField("num_values", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true) ::
        Nil
    )
    if (rpcName == "complete_row_from_id") {
      (returnType, (rowSchema, iter) => completeRowFromId(rowSchema, iter))
    } else if (rpcName == "complete_row_from_datum") {
      (returnType, completeRowFromDatum)
    } else {
      val msg = s"unknown RPC method: '$rpcName'"
      createLogger.warn(msg)
      throw new RuntimeException(msg)
    }
  }

  def completeRowFromId(rowSchema: Map[String, (Int, DataType)],
                        iter: Iterator[Row]): Iterator[Row] = {
    // set up Jubatus client and logger
    val client = new RecommenderClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started RecommenderClient: $client")

    // loop over the data as long as the Spark driver is not stopped
    var stopped_? = HttpClientPerJvm.stopped
    if (stopped_?) {
      logger.debug("driver status is 'stopped', skip processing")
    }
    var batchStartTime = System.currentTimeMillis()

    var resultSeq: Seq[Row] = List.empty[Row]
    var idx: Int = 0
    while (iter.hasNext && !stopped_?) {
      try {
        val row = iter.next
        val id = extractIdOrLabel(idColumnName, rowSchema, row, logger)
        val fullDatum = retry(2, logger)(client.completeRowFromId(id))
        val wrappedFullDatum = datumToJson(fullDatum)

        // every 1000 items, check if the Spark driver is still running
        if ((idx + 1) % 1000 == 0) {
          val duration = System.currentTimeMillis() - batchStartTime
          logger.debug(s"processed 1000 items using 'complete_row_from_id' method in $duration ms")
          stopped_? = HttpClientPerJvm.stopped
          if (stopped_?) {
            logger.debug("driver status is 'stopped', end processing")
          }
          batchStartTime = System.currentTimeMillis()
        }

        // we must add a nested row (not case class) to allow for nested queries
       val fromIdRow =  Row.fromSeq(row :+ Row.fromSeq(wrappedFullDatum.productIterator.toSeq))
        resultSeq = resultSeq :+ fromIdRow
      } catch {
        case e: Exception =>
          logger.error(s"Failed to completeRowFromId row.", e)
      } finally {
        idx += 1
      }
    }
    resultSeq.iterator
  }

  def completeRowFromDatum(rowSchema: Map[String, (Int, DataType)],
                           iter: Iterator[Row]): Iterator[Row] = {
    // set up Jubatus client and logger
    val client = new RecommenderClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started RecommenderClient: $client")

    // loop over the data as long as the Spark driver is not stopped
    var stopped_? = HttpClientPerJvm.stopped
    if (stopped_?) {
      logger.debug("driver status is 'stopped', skip processing")
    }
    var batchStartTime = System.currentTimeMillis()

    var resultSeq: Seq[Row] = List.empty[Row]
    var idx: Int = 0
    while (iter.hasNext && !stopped_?) {
      try {
        val row = iter.next
        // convert to datum
        val datum = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
        val fullDatum = retry(2, logger)(client.completeRowFromDatum(datum))
        val wrappedFullDatum = datumToJson(fullDatum)

        // every 1000 items, check if the Spark driver is still running
        if ((idx + 1) % 1000 == 0) {
          val duration = System.currentTimeMillis() - batchStartTime
          logger.debug(s"processed 1000 items using 'complete_row_from_datum' method in $duration ms")
          stopped_? = HttpClientPerJvm.stopped
          if (stopped_?) {
            logger.debug("driver status is 'stopped', end processing")
          }
          batchStartTime = System.currentTimeMillis()
        }

        // we must add a nested row (not case class) to allow for nested queries
        val fromDatumRow = Row.fromSeq(row :+ Row.fromSeq(wrappedFullDatum.productIterator.toSeq))
        resultSeq = resultSeq :+ fromDatumRow
      } catch {
        case e: Exception =>
          logger.error(s"Failed to completeRowFromDatum row.", e)
      } finally {
        idx += 1
      }
    }
    resultSeq.iterator
  }

  protected def datumToJson(datum: Datum): DatumResult = {
    DatumResult(
      datum.getStringValues().map(v => (v.key, v.value)).toMap,
      datum.getNumValues().map(v => (v.key, v.value)).toMap
    )
  }
}
