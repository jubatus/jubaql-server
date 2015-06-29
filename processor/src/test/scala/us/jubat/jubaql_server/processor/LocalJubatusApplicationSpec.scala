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

import org.scalatest._
import us.jubat.yarn.common.LearningMachineType
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

class LocalJubatusApplicationSpec extends FlatSpec with ShouldMatchers {

  val anomalyConfig = """{
  "method" : "lof",
  "parameter" : {
    "nearest_neighbor_num" : 10,
    "reverse_nearest_neighbor_num" : 30,
    "method" : "euclid_lsh",
    "parameter" : {
      "hash_num" : 64,
      "table_num" : 4,
      "seed" : 1091,
      "probe_num" : 64,
      "bin_width" : 100,
      "retain_projection" : false
    }
  },
  "converter" : {
    "string_filter_types" : {},
    "string_filter_rules" : [],
    "num_filter_types" : {},
    "num_filter_rules" : [],
    "string_types" : {},
    "string_rules" : [
      { "key" : "*", "type" : "str", "sample_weight" : "bin", "global_weight" : "bin" }
    ],
    "num_types" : {},
    "num_rules" : [
      { "key" : "*", "type" : "num" }
    ]
  }
}"""

  val classifierConfig = """{
  "method" : "AROW",
  "parameter" : {
    "regularization_weight" : 1.0
  },
  "converter" : {
    "string_filter_types" : {},
    "string_filter_rules" : [],
    "num_filter_types" : {},
    "num_filter_rules" : [],
    "string_types" : {},
    "string_rules" : [
      { "key" : "*", "type" : "str", "sample_weight" : "bin", "global_weight" : "bin" }
    ],
    "num_types" : {},
    "num_rules" : [
      { "key" : "*", "type" : "num" }
    ]
  }
}"""

  val recommenderConfig = """{
  "method": "lsh",
  "parameter" : {
    "hash_num" : 64
  },
  "converter" : {
    "string_filter_types": {},
    "string_filter_rules":[],
    "num_filter_types": {},
    "num_filter_rules": [],
    "string_types": {},
    "string_rules":[
      {"key" : "*", "type" : "str", "sample_weight":"bin", "global_weight" : "bin"}
    ],
    "num_types": {},
    "num_rules": [
      {"key" : "*", "type" : "num"}
    ]
  }
}"""

  "mkfifo" should "make a named pipe" taggedAs (LocalTest) in {
    import java.io._

    val namedPipePath = "/tmp/abcdefghijklmnopqrstuvwxyz"
    val namedPipe = new File(namedPipePath)
    try {
      val runtime = Runtime.getRuntime
      namedPipe.exists shouldBe false
      LocalJubatusApplication.mkfifo(namedPipePath, runtime)
      namedPipe.exists shouldBe true

      val process = runtime.exec(s"file $namedPipePath")
      val in = process.getInputStream
      val br = new BufferedReader(new InputStreamReader(in))
      val line = br.readLine
      line should not be null
      // The following code depends on file command,
      // so may fail on some environment.
      line.contains("named pipe") shouldBe true
    } finally {
      namedPipe.delete()
    }
  }

  "jubaanomaly" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("foo", LearningMachineType.Anomaly, anomalyConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  "jubaclassifier" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  it should "be startable twice" taggedAs (LocalTest, JubatusTest) in {
    val f1 = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f1, Duration.Inf)
    val result1 = f1.value.get
    val f2 = LocalJubatusApplication.start("bar", LearningMachineType.Classifier, classifierConfig)
    Await.ready(f2, Duration.Inf)
    val result2 = f2.value.get
    result1 shouldBe a[Success[_]]
    result2 shouldBe a[Success[_]]
    result1.get.jubatusProxy.port shouldBe 9199
    result2.get.jubatusProxy.port shouldBe 9200
    result1 match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
    result2 match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }

  "jubarecommender" should "start" taggedAs (LocalTest, JubatusTest) in {
    val f = LocalJubatusApplication.start("baz", LearningMachineType.Recommender, recommenderConfig)
    Await.ready(f, Duration.Inf)
    val result = f.value.get
    result shouldBe a[Success[_]]
    result match {
      case Success(app) =>
        Await.ready(app.stop(), Duration.Inf)
      case _ =>
    }
  }
}
