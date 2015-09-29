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

import org.scalatest.{Suite, BeforeAndAfterAll}
import scala.sys.process.ProcessLogger

trait GatewayServer extends BeforeAndAfterAll with HasSpark {
  this: Suite =>

  protected val plan = new GatewayPlan("example.com", 1234,
                                       Array(), RunMode.Test,
                                       sparkDistribution = "",
                                       fatjar = "src/test/resources/processor-logfile.jar",
                                       checkpointDir = "file:///tmp/spark")
  protected val server = unfiltered.netty.Server.http(9877).plan(plan)

  //run.mode=production指定サーバ
  protected val pro_plan = new GatewayPlan("example.com", 1235,
                                       Array(), RunMode.Production("localhost"),
                                       sparkDistribution = sparkPath,
                                       fatjar = "src/test/resources/processor-logfile.jar",
                                       checkpointDir = "file:///tmp/spark")
  protected val pro_server = unfiltered.netty.Server.http(9878).plan(pro_plan)

  //run.mode=development指定サーバ
  protected val dev_plan = new GatewayPlan("example.com", 1236,
                                       Array(), RunMode.Development(1),
                                       sparkDistribution = sparkPath,
                                       fatjar = "src/test/resources/processor-logfile.jar",
                                       checkpointDir = "file:///tmp/spark")
  protected val dev_server = unfiltered.netty.Server.http(9879).plan(dev_plan)

  override protected def beforeAll() = {
    server.start()
    pro_server.start()
    dev_server.start()
  }

  override protected def afterAll() = {
    server.stop()
    pro_server.stop()
    dev_server.stop()
  }

}
