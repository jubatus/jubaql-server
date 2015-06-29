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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types.{DataType, DoubleType}
import us.jubat.anomaly.AnomalyClient
import scala.collection.concurrent

class Anomaly(val jubaHost: String, jubaPort: Int, cm: CreateModel, featureFunctions: concurrent.Map[String, String])
  extends JubatusClient with Serializable {

  // TODO wrap all network calls to Jubatus in try{} blocks

  override def update(rowSchema: Map[String, (Int, DataType)],
                      iter: Iterator[Row]): Unit = {
    // set up Jubatus client and logger
    val client = new AnomalyClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started AnomalyClient: $client")

    // loop over the data as long as the Spark driver is not stopped
    var stopped_? = HttpClientPerJvm.stopped
    if (stopped_?) {
      logger.debug("driver status is 'stopped', skip processing")
    }
    var batchStartTime = System.currentTimeMillis()
    iter.takeWhile(_ => !stopped_?).zipWithIndex.foreach { case (row, idx) => {
      // create datum and send to Jubatus
      val d = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
      retry(2, logger)(client.add(d))

      // every 1000 items, check if the Spark driver is still running
      if ((idx + 1) % 1000 == 0) {
        val duration = System.currentTimeMillis() - batchStartTime
        logger.debug(s"processed 1000 items using 'add' method in $duration ms")
        stopped_? = HttpClientPerJvm.stopped
        if (stopped_?) {
          logger.debug("driver status is 'stopped', end processing")
        }
        batchStartTime = System.currentTimeMillis()
      }
    }
    }
  }

  override def analyzeMethod(rpcName: String) = {
    if (rpcName != "calc_score") {
      createLogger.warn(s"unknown RPC method: '$rpcName', using 'calc_score'")
    }
    val returnType = DoubleType
    (returnType, calcScore _)
  }

  def calcScore(rowSchema: Map[String, (Int, DataType)],
                iter: Iterator[Row]): Iterator[Row] = {
    // set up Jubatus client and logger
    val client = new AnomalyClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started AnomalyClient: $client")

    // loop over the data as long as the Spark driver is not stopped
    var stopped_? = HttpClientPerJvm.stopped
    if (stopped_?) {
      logger.debug("driver status is 'stopped', skip processing")
    }
    var batchStartTime = System.currentTimeMillis()
    iter.takeWhile(_ => !stopped_?).zipWithIndex.map { case (row, idx) => {
      // convert to datum and compute score via Jubatus
      val datum = DatumExtractor.extract(cm, rowSchema, row, featureFunctions, logger)
      // if we return a Float here, this will result in casting exceptions
      // during processing, so we convert to double
      val score = retry(2, logger)(client.calcScore(datum).toDouble)
      // we may get an Infinity result if this row is identical to too many
      // other items, cf. <https://github.com/jubatus/jubatus_core/issues/130>.
      // we assume 1.0 instead to avoid weird behavior in the future if the
      // infinity value appeared in the row.
      val adjustedScore = if (score.isInfinite) {
        1.0
      } else {
        score
      }

      // every 1000 items, check if the Spark driver is still running
      if ((idx + 1) % 1000 == 0) {
        val duration = System.currentTimeMillis() - batchStartTime
        logger.debug(s"processed 1000 items using 'calc_score' method in $duration ms")
        stopped_? = HttpClientPerJvm.stopped
        if (stopped_?) {
          logger.debug("driver status is 'stopped', end processing")
        }
        batchStartTime = System.currentTimeMillis()
      }

      Row.fromSeq(row :+ adjustedScore)
    }
    }
  }
}
