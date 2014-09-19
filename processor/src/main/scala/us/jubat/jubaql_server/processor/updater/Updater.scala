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

import org.json4s._
import us.jubat.common.Datum
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

trait Updater {
  def apply(iter: scala.Iterator[JValue], statusUrl: String): Iterator[Unit]

  protected def createLogger: Logger = {
    Logger(LoggerFactory getLogger getClass.getName)
  }

  protected def extractDatum(keys: List[String], jvalue: JValue): Datum = {
    // filter unused filed
    val filtered = jvalue.filterField {
      case JField(key, _) => keys.indexOf(key) >= 0
      case _ => false
    }

    var datum = new Datum
    filtered.foreach({
      j =>
        val key = j._1
        j._2 match {
          case JInt(v) =>
            datum.addNumber(key, v.toDouble)
          case JDouble(v) =>
            datum.addNumber(key, v)
          case JString(v) =>
            datum.addString(key, v)
          case _ =>
        }
        j
    })
    datum
  }
}
