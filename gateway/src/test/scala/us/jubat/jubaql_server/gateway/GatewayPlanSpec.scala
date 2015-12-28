// Jubatus: Online machine learning framework for distributed environment
// Copyright (C) 2015 Preferred Networks and Nippon Telegraph and Telephone Corporation.
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

import org.scalatest._
import dispatch._
import dispatch.Defaults._
import scala.util.Success
import unfiltered.netty.Server

class GatewayPlanSpec extends FlatSpec with Matchers with HasSpark {

  "startingTimeout propery non exist" should "reflect default value" in {
    // System.setProperty("", arg1)
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0,0 )
    plan.submitTimeout shouldBe (60000)
  }

  "startingTimeout propery exist" should "reflect value" in {
    System.setProperty("jubaql.gateway.submitTimeout", "12345")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0,0 )
    plan.submitTimeout shouldBe (12345)
  }

  "startingTimeout propery illegal value" should "reflect default value" in {
    System.setProperty("jubaql.gateway.submitTimeout", "fail")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0,0 )
    plan.submitTimeout shouldBe (60000)
  }

  "startingTimeout propery negative value" should "reflect default value" in {
    System.setProperty("jubaql.gateway.submitTimeout", "-30000")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0,0 )
    plan.submitTimeout shouldBe (60000)
  }

  "JubaQL Processor's log conf propery non exist" should "reflect default value(None)" in {
    // System.setProperty("jubaql.processor.logconf", "default")
    System.clearProperty("jubaql.processor.logconf")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0, 0)
    plan.processorLogConfigPath shouldBe (None)
  }

  "JubaQL Processor's log conf propery empty value" should "reflect default value(None)" in {
    System.clearProperty("jubaql.processor.logconf")
    System.setProperty("jubaql.processor.logconf", "")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0, 0)
    plan.processorLogConfigPath shouldBe (None)
  }

  "JubaQL Processor's log conf propery exist" should "reflect value" in {
    System.clearProperty("jubaql.processor.logconf")
    System.setProperty("jubaql.processor.logconf", "file:///jubaql/conf/processorLogConf_log4j.xml")
    val plan = new GatewayPlan("example.com", 1234,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:9877", false, 16, 0, 0)
     plan.processorLogConfigPath should not be (None)
    plan.processorLogConfigPath.get shouldBe ("file:///jubaql/conf/processorLogConf_log4j.xml")
  }
}