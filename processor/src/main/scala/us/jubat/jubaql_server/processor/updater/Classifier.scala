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

import us.jubat.jubaql_server.processor.{DatumExtractor, CreateModel}
import us.jubat.jubaql_server.processor.json.ClassifierPrediction
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types._
import us.jubat.classifier.{ClassifierClient, LabeledDatum}
import us.jubat.common.Datum

import scala.collection.JavaConversions._
import scala.collection.concurrent
import scala.util.{Failure, Success, Try}

class Classifier(val jubaHost: String, jubaPort: Int, cm: CreateModel,
                 featureFunctions: concurrent.Map[String, String],
                 val labelColumnName: String)
  extends JubatusClient with Serializable {

  // TODO wrap all network calls to Jubatus in try{} blocks

  override def update(rowSchema: Map[String, (Int, DataType)],
                      iter: Iterator[Row]): Unit = {
    // set up Jubatus client and logger
    val client = new ClassifierClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started ClassifierClient: $client")

    // get the index of the label column (if it exists)
    rowSchema.get(labelColumnName) match {
      case None =>
        val msg = "the given schema %s does not contain a column named '%s'".format(
          rowSchema, labelColumnName)
        logger.error(msg)
        throw new RuntimeException(msg)

      case Some((labelIdx, _)) =>
        // loop over the data as long as the Spark driver is not stopped
        var stopped_? = HttpClientPerJvm.stopped
        if (stopped_?) {
          logger.debug("driver status is 'stopped', skip processing")
        }
        var batchStartTime = System.currentTimeMillis()
        // TODO we can make this more efficient using batch training
        iter.takeWhile(_ => !stopped_?).zipWithIndex.foreach { case (row, idx) => {
          if (!row.isNullAt(labelIdx)) {
            // create datum and send to Jubatus
            val labelValue = row.getString(labelIdx)
            val datum = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
            val labelDatum = new LabeledDatum(labelValue, datum)
            val datumList = new java.util.LinkedList[LabeledDatum]()
            datumList.add(labelDatum)
            retry(2, logger)(client.train(datumList))

            // every 1000 items, check if the Spark driver is still running
            if ((idx + 1) % 1000 == 0) {
              val duration = System.currentTimeMillis() - batchStartTime
              logger.debug(s"processed 1000 items using 'train' method in $duration ms")
              stopped_? = HttpClientPerJvm.stopped
              if (stopped_?) {
                logger.debug("driver status is 'stopped', end processing")
              }
              batchStartTime = System.currentTimeMillis()
            }
          } else {
            logger.warn("row %s has a NULL label".format(row))
          }
        }
        }
    }
  }

  override def analyzeMethod(rpcName: String) = {
    if (rpcName != "classify") {
      createLogger.warn(s"unknown RPC method: '$rpcName', using 'classify'")
    }
    // maybe we want to change this to MapType later on
    val returnType = ArrayType(StructType(
      StructField("label", StringType, nullable = false) ::
        StructField("score", DoubleType, nullable = false) ::
        Nil
    ))
    (returnType, classify _)
  }

  def classify(rowSchema: Map[String, (Int, DataType)],
               iter: Iterator[Row]): Iterator[Row] = {
    // set up Jubatus client and logger
    val client = new ClassifierClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started ClassifierClient: $client")

    // loop over the data as long as the Spark driver is not stopped
    var stopped_? = HttpClientPerJvm.stopped
    if (stopped_?) {
      logger.debug("driver status is 'stopped', skip processing")
    }
    var batchStartTime = System.currentTimeMillis()
    iter.takeWhile(_ => !stopped_?).zipWithIndex.flatMap { case (row, idx) => {
      // TODO we can make this more efficient using batch training
      // convert to datum
      val datum = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
      val datumList = new java.util.LinkedList[Datum]()
      datumList.add(datum)
      // classify
      val maybeClassifierResult = retry(2, logger)(client.classify(datumList).toList) match {
        case labeledDatumList :: rest =>
          if (!rest.isEmpty) {
            logger.warn("received more than one result from classifier, " +
              "ignoring all but the first")
          }
          if (labeledDatumList.isEmpty) {
            logger.warn("got an empty classification list for datum")
          }
          Some(labeledDatumList.map(labeledDatum => {
            ClassifierPrediction(labeledDatum.label, labeledDatum.score)
          }).toList)
        case Nil =>
          logger.error("received no result from classifier")
          None
      }

      // every 1000 items, check if the Spark driver is still running
      if ((idx + 1) % 1000 == 0) {
        val duration = System.currentTimeMillis() - batchStartTime
        logger.debug(s"processed 1000 items using 'classify' method in $duration ms")
        stopped_? = HttpClientPerJvm.stopped
        if (stopped_?) {
          logger.debug("driver status is 'stopped', end processing")
        }
        batchStartTime = System.currentTimeMillis()
      }
      maybeClassifierResult.map(classifierResult => {
        Row.fromSeq(row :+ classifierResult.map(r =>
          Row.fromSeq(r.productIterator.toSeq)))
      })
    }
    }
  }
}
