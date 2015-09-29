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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.Expression

sealed trait JubaQLAST

case class
CreateDatasource(sourceName: String,
                 columns: List[(String, String)],
                 sinkStorage: String,
                 sinkStreams: List[String]) extends JubaQLAST

case class
CreateModel(algorithm: String,
            modelName: String,
            labelOrId: Option[(String, String)],
            featureExtraction: List[(FeatureFunctionParameters, String)],
            configJson: String) extends JubaQLAST {
  override def toString: String = "CreateModel(%s,%s,%s,%s,%s)".format(
    algorithm,
    modelName,
    labelOrId,
    featureExtraction,
    if (configJson.size > 13) configJson.take(5) + "..." + configJson.takeRight(5)
    else configJson
  )
}

case class Update(modelName: String, rpcName: String, source: String) extends JubaQLAST

case class CreateStreamFromSelect(streamName: String, selectPlan: LogicalPlan) extends JubaQLAST

case class CreateStreamFromAnalyze(streamName: String, analyze: Analyze, newColumn: Option[String]) extends JubaQLAST

case class CreateTrigger(dsName: String, condition: Option[Expression], expr: Expression) extends JubaQLAST

case class CreateStreamFromSlidingWindow(streamName: String, windowSize: Int, slideInterval: Int,
                                         windowType: String, source: LogicalPlan,
                                         funcSpecs: List[(String, List[Expression], Option[String])],
                                         postCond: Option[Expression]) extends JubaQLAST

case class Analyze(modelName: String, rpcName: String, data: String) extends JubaQLAST

case class LogStream(streamName: String) extends JubaQLAST

case class Status() extends JubaQLAST

case class Shutdown() extends JubaQLAST

case class StartProcessing(dsName: String) extends JubaQLAST

case class StopProcessing() extends JubaQLAST

case class CreateFunction(funcName: String, args: List[(String, String)],
                          returnType: String, lang: String, body: String) extends JubaQLAST

case class CreateFeatureFunction(funcName: String, args: List[(String, String)],
                                 lang: String, body: String) extends JubaQLAST

case class CreateTriggerFunction(funcName: String, args: List[(String, String)],
                                 lang: String, body: String) extends JubaQLAST

case class SaveModel(modelName: String, modelPath: String, modelId: String) extends JubaQLAST

case class LoadModel(modelName: String, modelPath: String, modelId: String) extends JubaQLAST
