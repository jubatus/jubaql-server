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

import dispatch.Defaults._
import dispatch._
import us.jubat.jubaql_server.gateway.json.Register
import org.json4s._
import org.json4s.native.Serialization.write
import org.scalatest._
import EitherValues._

class UnregisterSpec extends FlatSpec with Matchers with GatewayServer with BeforeAndAfter {

  implicit val formats = DefaultFormats

  val loginUrl  = :/("localhost", 9877) / "login"
  val registrationUrl = :/("localhost", 9877) / "registration"

  val registerJson = write(Register("register", "8.8.8.8", 30)).toString
  val unregisterJson = """{"action": "unregister"}"""

  def requestAsJson(key: String) = {
    val request = (registrationUrl / key).POST
    request.setContentType("application/json", "UTF-8")
  }

  before {
    // login twice
    for (i <- 0 until 2) {
      val req = Http(loginUrl.POST OK as.String)
      req.option.apply()
    }

    // register all keys
    for (key <- keys) {
      val requestWithBody = requestAsJson(key) << registerJson
      Http(requestWithBody OK as.String).option.apply()
    }
  }

  after {
    // unregister all keys
    for (key <- keys) {
      val requestWithBody = requestAsJson(key) << unregisterJson
      Http(requestWithBody OK as.String).option.apply()
    }
  }

  def keys = {
    var keys = List.empty[String]
    // This lock is required because the server is in the same process.
    plan.session2key.synchronized {
      for (key <- plan.key2session.keys)
        keys = key :: keys
    }
    keys
  }

  "Unregistering with not a JSON" should "fail" in {
    for (key <- keys) {
      val requestWithNotAJson = requestAsJson(key) << "abc"
      val result = Http(requestWithNotAJson OK as.String).either.apply()
      result.left.value.getMessage shouldBe "Unexpected response status: 400"
    }
  }

  "Unregistering with a JSON which in not for unregisttering" should "fail" in {
    for (key <- keys) {
      val requestWithBadJson = requestAsJson(key) << "{}"
      val result = Http(requestWithBadJson OK as.String).either.apply()
      result.left.value.getMessage shouldBe "Unexpected response status: 400"
    }
  }

  "Unregistering a nonexistent key" should "succeed" in {
    for (key <- keys) {
      val garbageKey = key + "garabage"
      val requestWithBody = requestAsJson(garbageKey) << unregisterJson
      val result = Http(requestWithBody OK as.String).either.apply()
      result.right.value shouldBe "Successfully unregistered"
    }
  }

  "Unregistering an existing key" should "succeed" in {
    for (key <- keys) {
      var sessionId = ""
      plan.session2key.synchronized {
        sessionId = plan.key2session.get(key).get
      }
      val requestWithBody = requestAsJson(key) << unregisterJson
      Http(requestWithBody OK as.String).option.apply() should not be None
      plan.session2key.synchronized {
        plan.session2key.get(sessionId) shouldBe None
        plan.key2session.get(key) shouldBe None
        plan.session2loc.get(sessionId) shouldBe None
      }
    }
  }
}
