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
package us.jubat.jubaql_server.gateway

import java.io.{FileInputStream, FileNotFoundException}
import java.util.Properties

import org.scalatest._

trait HasSpark extends ShouldMatchers {
  lazy val sparkPath: String = {
    val sparkConfig = "src/test/resources/spark.xml"

    val is = try {
      Some(new FileInputStream(sparkConfig))
    } catch {
      case _: FileNotFoundException =>
        None
    }
    is shouldBe a[Some[_]]

    val properties = new Properties()
    properties.loadFromXML(is.get)

    properties.getProperty("spark_home_path")
  }
}
