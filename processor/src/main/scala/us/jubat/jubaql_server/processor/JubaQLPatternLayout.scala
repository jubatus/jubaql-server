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

import java.net.InetAddress

import org.apache.log4j.PatternLayout
import org.apache.log4j.helpers.{PatternConverter, PatternParser}
import org.apache.log4j.spi.LoggingEvent

class JubaQLPatternLayout extends PatternLayout {
  val hostname = InetAddress.getLocalHost().getHostName

  override def createPatternParser(pattern: String): PatternParser = {
    new PatternParser(pattern) {
      override def finalizeConverter(c: Char): Unit = {
        c match {
          // add a new 'h' pattern to the conversion string
          case 'h' =>
            val pc = new PatternConverter {
              override def convert(event: LoggingEvent): String = {
                hostname
              }
            }
            addConverter(pc)
          // all other characters are handled by the original pattern parser
          case other =>
            super.finalizeConverter(other)
        }
      }
    }
  }
}
