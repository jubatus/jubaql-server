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

import us.jubat.jubaql_server.processor.CreateModel
import org.json4s._
import us.jubat.anomaly.AnomalyClient

class Anomaly(val jubaHost: String, jubaPort: Int, cm: CreateModel, val keys: List[String]) extends Updater with Serializable {
  override def apply(iter: Iterator[JValue], statusUrl: String): Iterator[Unit] = {
    HttpClientPerJvm.startChecking(statusUrl)
    val client = new AnomalyClient(jubaHost, jubaPort, cm.modelName, 5)
    val logger = createLogger
    logger.info(s"started AnomalyClient: $client")
    var stopped_? = HttpClientPerJvm.stopped
    val out = iter.takeWhile(_ => !stopped_?).zipWithIndex.map(valueWithIndex => {
      val (jvalue, idx) = valueWithIndex
      client.add(extractDatum(keys, jvalue))
      if ((idx+1) % 1000 == 0) {
        logger.debug("processed 1000 items using 'add' method")
        stopped_? = HttpClientPerJvm.stopped
      }
      ()
    })
    out
  }
}
