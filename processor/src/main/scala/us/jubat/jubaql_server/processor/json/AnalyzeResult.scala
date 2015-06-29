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

// We use a sealed trait to make sure we have all possible
// response types in *this* file.
sealed trait AnalyzeResult

case class AnomalyScore(score: Float)
  extends AnalyzeResult

case class ClassifierPrediction(label: String, score: Double)

case class ClassifierResult(predictions: List[ClassifierPrediction])
  extends AnalyzeResult

case class DatumResult(string_values: Map[String, String],
                       num_values: Map[String, Double])
  extends AnalyzeResult
