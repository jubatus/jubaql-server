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
import dispatch._
import dispatch.Defaults._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import us.jubat.jubaql_server.processor.json.{Unregister, Register}
import scala.util.{Failure, Success, Try}

class RegistrationHandler(val registerUrl: String) extends LazyLogging {
  // RequestBuilder is mutable, therefore we need to work on a new copy
  // for every request (i.e. use `def` instead of `val`).
  def newReq = url(registerUrl)

  implicit val formats = DefaultFormats

  def register: Either[Throwable, String] = {
    val (host, port) = JubaQLProcessor.getListeningAddress
    val registerMsg = Register("register", host.getHostAddress, port)
    val req = newReq << Serialization.write(registerMsg)
    makeHttpRequest(req)
  }

  def unregister: Either[Throwable, String] = {
    val unregisterMsg = Unregister("unregister")
    val req = newReq << Serialization.write(unregisterMsg)
    makeHttpRequest(req)
  }

  protected def makeHttpRequest(req: Req) = {
    Try {
      // if the URL is invalid, that exception will be thrown instead
      // of wrapped into Either, so we have to wrap it ourselves
      Http(req OK as.String).either.apply
    } match {
      case Success(someEither) =>
        someEither
      case Failure(someException) =>
        Left(someException)
    }
  }
}
