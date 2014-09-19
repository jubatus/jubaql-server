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


sealed abstract trait JubaQLAST

case class
CreateDatasource(sourceName: String,
                 columns: List[(String, String)],
                 sinkStorage: String,
                 sinkStreams: List[String]) extends JubaQLAST

case class
CreateModel(algorithm: String,
            modelName: String,
            configJson: String,
            specifier: List[(String, List[String])]) extends JubaQLAST {
  override def toString: String = "CreateModel(%s,%s,%s,%s)".format(
    algorithm,
    modelName,
    if (configJson.size > 13) configJson.take(5) + "..." + configJson.takeRight(5)
    else configJson,
    specifier
  )
}

case class Update(modelName: String, rpcName: String, source: String) extends JubaQLAST

case class Analyze(modelName: String, rpcName: String, data: String) extends JubaQLAST

case class Shutdown() extends JubaQLAST

case class StopProcessing() extends JubaQLAST
