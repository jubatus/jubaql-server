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

import org.apache.spark.sql.DataType
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.StructType
import scala.concurrent.Future
import us.jubat.yarn.client.JubatusYarnApplication
import us.jubat.jubaql_server.processor.updater.JubatusClient

sealed trait PreparedJubaQLStatement

case class PreparedUpdate(modelName: String,
                          modelFut: Future[JubatusYarnApplication],
                          dataSourceName: String,
                          updaterFut: Future[JubatusClient]) extends PreparedJubaQLStatement

case class PreparedCreateStreamFromSelect(streamName: String,
                              selectPlan: LogicalPlan,
                              usedTables: List[String],
                              selectString: String) extends PreparedJubaQLStatement {
  override def toString: String = {
    "PreparedCreateStreamFromSelect(%s,<SELECT ...>,%s)".format(streamName, usedTables)
  }
}

case class PreparedCreateStreamFromAnalyze(streamName: String,
                                           modelName: String,
                                           modelFut: Future[JubatusYarnApplication],
                                           dataSourceName: String,
                                           analyzerFut: Future[JubatusClient],
                                           rpcName: String,
                                           newColumn: Option[String]) extends PreparedJubaQLStatement

case class PreparedCreateTrigger(dsName: String, condition: Option[Expression], expr: Expression) extends PreparedJubaQLStatement

case class PreparedCreateStreamFromSlidingWindow(streamName: String,
                                                 windowSize: Int,
                                                 slideInterval: Int,
                                                 windowType: String,
                                                 source: LogicalPlan,
                                                 funcSpecs: List[SomeAggregateFunction[_]],
                                                 outSchema: StructType,
                                                 postCond: Option[Expression],
                                                 functionString: String) extends PreparedJubaQLStatement

case class PreparedLogStream(streamName: String) extends PreparedJubaQLStatement
