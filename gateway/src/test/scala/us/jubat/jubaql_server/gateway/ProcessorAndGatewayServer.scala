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

import us.jubat.jubaql_server.gateway.json.QueryToProcessor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor
import unfiltered.request._
import unfiltered.response._
import unfiltered.netty.{cycle, ServerErrorResponse}
import unfiltered.util.RunnableServer
import org.scalatest.{Suite, BeforeAndAfterAll}
import org.json4s._

trait ProcessorAndGatewayServer extends GatewayServer {
  this: Suite =>

  val session = "TESTSESSIONID"
  // This variable does not have name "key" due to multiple inheritance conflict
  val key_ = "KEY"
  val loc = ("localhost", 9876)

  protected val processorMock: RunnableServer =
    unfiltered.netty.Server.http(9876).plan(
      new ProcessorMockPlan
    )

  override def beforeAll(): Unit = {
    plan.sessionManager.session2key += (session -> key_)
    plan.sessionManager.key2session += (key_ -> session)
    plan.sessionManager.session2loc += (session -> loc)
    super.beforeAll()

    processorMock.start()
  }

  override def afterAll(): Unit = {
    processorMock.stop()

    super.afterAll()
  }
}

class ProcessorMockPlan
  extends cycle.Plan
  with cycle.DeferralExecutor with cycle.DeferredIntent
  with ServerErrorResponse
  with LazyLogging {

  lazy val underlying = new MemoryAwareThreadPoolExecutor(16, 65536, 1048576)

  implicit val formats = DefaultFormats

  def intent = {
    case req@POST(Path("/jubaql")) =>
      val maybeJson = JsonBody(req)
      val maybeQuery = maybeJson.flatMap(_.extractOpt[QueryToProcessor])
      maybeQuery match {
        case None =>
          BadRequest ~> ResponseString("Valid JSON is required")
        case Some(query) =>
          val queryString = query.query
          Ok ~> ResponseString("Valid JSON")
      }
  }
}
