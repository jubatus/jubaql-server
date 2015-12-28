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
package us.jubat.jubaql_server.processor.json

import scala.collection.Map

// We use a sealed trait to make sure we have all possible
// response types in *this* file.
sealed trait JubaQLResponse

case class StatementProcessed(result: String)
  extends JubaQLResponse

case class AnalyzeResultWrapper(result: AnalyzeResult)
  extends JubaQLResponse

case class StatusResponse(result: String,
  sources: Map[String, Any],
  models: Map[String, Any],
  processor: Map[String, Any],
  streams: Map[String, Map[String, Any]])

  extends JubaQLResponse
