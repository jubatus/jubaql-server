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
package us.jubat.jubaql_server.processor.logical

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, LogicalPlan}

/*
 * This logical plan has only the purpose of triggering the
 * assignment of a table name to this RDD after processing
 * (i.e., exists so that we can recognize that this is a
 * "CREATE JSON_TABLE" statement even outside of the parser).
 */
case class RegisterAsTable(child: LogicalPlan, tableName: String) extends UnaryNode {
  override def output = child.output
}
