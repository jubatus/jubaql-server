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

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.finagle.{SimpleFilter, Service}
import io.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class HandleExceptions
  extends SimpleFilter[HttpRequest, HttpResponse]
  with LazyLogging {
  def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
    // `handle` asynchronously handles exceptions.
    service(request) handle {
      case error =>
        logger.error(error.toString)
        logger.error(error.getMessage)
        logger.error(error.getStackTraceString)
        val statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR
        val body = ("result" -> error.getMessage)
        val errorResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, statusCode)
        errorResponse.setContent(ChannelBuffers.copiedBuffer(compact(render(body)), CharsetUtil.UTF_8))
        errorResponse
    }
  }
}
