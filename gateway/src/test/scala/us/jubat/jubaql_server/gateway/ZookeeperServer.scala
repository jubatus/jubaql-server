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

import org.scalatest.{Suite, BeforeAndAfterAll, BeforeAndAfter}
import org.apache.curator.test.TestingServer

trait ZookeeperServer extends BeforeAndAfter with BeforeAndAfterAll {
  this: Suite =>

  val zkServer = new TestingServer(2181, true)

  before {
    zkServer.restart()
  }
  after {
    zkServer.stop()
  }

  override protected def afterAll() {
    zkServer.close()
  }
}