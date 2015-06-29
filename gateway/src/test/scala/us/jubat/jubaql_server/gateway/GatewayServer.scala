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

trait GatewayServer extends BeforeAndAfterAll {
  this: Suite =>

  protected val plan = new GatewayPlan("example.com", 1234,
                                       Array(), RunMode.Test,
                                       sparkDistribution = "",
                                       fatjar = "src/test/resources/processor-logfile.jar",
                                       checkpointDir = "file:///tmp/spark")
  protected val server = unfiltered.netty.Server.http(9877).plan(plan)

  override protected def beforeAll() = {
    server.start()
  }

  override protected def afterAll() = {
    server.stop()
  }
}
