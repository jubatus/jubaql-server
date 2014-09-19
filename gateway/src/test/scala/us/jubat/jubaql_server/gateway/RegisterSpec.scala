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

import us.jubat.jubaql_server.gateway.json.Register
import org.scalatest._
import org.json4s._
import dispatch._
import dispatch.Defaults._
import org.json4s.Formats._
import org.json4s.native.Serialization.{read, write}
import org.json4s.native.JsonMethods._
import EitherValues._

class RegisterSpec extends FlatSpec with Matchers with GatewayServer {

  implicit val formats = DefaultFormats

  val loginUrl  = :/("localhost", 9877) / "login"
  val registrationUrl = :/("localhost", 9877) / "registration"

  def requestAsJson(key: String) = {
    val request = (registrationUrl / key).POST
    request.setContentType("application/json", "UTF-8")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // login twice
    for (i <- 0 until 2) {
      val req = Http(loginUrl.POST OK as.String)
      req.option.apply()
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

  "Registering with not a JSON" should "fail" in {
    for (key <- keys) {
      val requestWithBody = requestAsJson(key) << "abc"
      val result = Http(requestWithBody OK as.String).either.apply()
      result.left.value.getMessage shouldBe "Unexpected response status: 400"
    }
  }

  "Registering with a JSON which is not for registering" should "fail" in {
    for (key <- keys) {
      val requestWithBody = requestAsJson(key) << """{"ip": "8.8.8.8", "port": 30}"""
      val result = Http(requestWithBody OK as.String).either.apply()
      result.left.value.getMessage shouldBe "Unexpected response status: 400"
    }
  }

  "Registering a nonexistent key" should "fail" in {
    for (key <- keys) {
      val garbageKey = key + "garbage"
      val registerJson = write(Register("register", "8.8.8.8", 30))
      val requestWithBody = requestAsJson(garbageKey) << registerJson.toString
      val result = Http(requestWithBody OK as.String).either.apply()
      result.left.value.getMessage shouldBe "Unexpected response status: 401"
    }
  }

  "Registering an existing key" should "succeed" in {
    for (key <- keys) {
      val registerJson = write(Register("register", "8.8.8.8", 30))
      val requestWithBody = requestAsJson(key) << registerJson.toString
      Http(requestWithBody OK as.String).option.apply() should not be None
      plan.session2key.synchronized {
        val session = plan.key2session.get(key)
        session should not be None
        val maybeLoc = plan.session2loc.get(session.get)
        maybeLoc should not be None
        val loc = maybeLoc.get
        loc shouldBe ("8.8.8.8", 30)
      }
    }
  }

  "Registering already registered settings" should "overwrite" in {
    for (key <- keys) {
      for (i <- 0 until 2) {
        val ip = "8.8.8." + (8 + i).toString
        val port = 30 + i
        val registerJson = write(Register("register", ip, port))
        val requestWithBody = requestAsJson(key) << registerJson.toString
        Http(requestWithBody OK as.String).option.apply() should not be None
        plan.session2key.synchronized {
          val session = plan.key2session.get(key)
          session should not be None
          val maybeLoc = plan.session2loc.get(session.get)
          maybeLoc should not be None
          val loc = maybeLoc.get
          loc shouldBe (ip, port)
        }
      }
    }
  }
}
