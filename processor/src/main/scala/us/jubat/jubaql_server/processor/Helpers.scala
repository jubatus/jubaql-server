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
import org.joda.time.format.{DateTimeFormatterBuilder, ISODateTimeFormat}

object Helpers {
  def niceRDDString(rdd: RDD[_]): String = {
    rdd.toDebugString.split('\n').map("  " + _).mkString("\n")
  }

  // a date parser for 2014-11-21T15:52:21[.943321112]
  protected val timestampParser = {
    val fractionElem = new DateTimeFormatterBuilder()
      .appendLiteral('.')
      .appendFractionOfSecond(3, 9).toFormatter
    new DateTimeFormatterBuilder()
      .append(ISODateTimeFormat.date)
      .appendLiteral('T')
      .append(ISODateTimeFormat.hourMinuteSecond)
      .appendOptional(fractionElem.getParser)
      .toFormatter
  }

  def parseTimestamp(s: String): Long = timestampParser.parseMillis(s)

  // a date formatter for 2014-11-21T15:52:21.943
  // note that this will only be used for window timestamps, so millisecond
  // precision is totally ok
  protected val timestampFormatter = ISODateTimeFormat.dateHourMinuteSecondMillis()

  def formatTimestamp(l: Long): String = timestampFormatter.print(l)
}
