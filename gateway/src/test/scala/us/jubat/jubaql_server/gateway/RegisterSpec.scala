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
import us.jubat.jubaql_server.gateway.json.SessionId

class RegisterSpec extends FlatSpec with Matchers with GatewayServer with BeforeAndAfter {

  implicit val formats = DefaultFormats

  val loginUrl  = :/("localhost", 9877) / "login"
  val registrationUrl = :/("localhost", 9877) / "registration"
  val persist_loginUrl  = :/("localhost", 9880) / "login"
  val persist_registrationUrl = :/("localhost", 9880) / "registration"

  def requestAsJson(key: String, url: Req = registrationUrl) = {
    val request = (url / key).POST
    request.setContentType("application/json", "UTF-8")
  }

  before {
    zkServer.restart
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
    plan.sessionManager.session2key.synchronized {
      for (key <- plan.sessionManager.key2session.keys)
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
      result.left.value.getMessage shouldBe "Unexpected response status: 500"
    }
  }

  "Registering an existing key" should "succeed" in {
    for (key <- keys) {
      val registerJson = write(Register("register", "8.8.8.8", 30))
      val requestWithBody = requestAsJson(key) << registerJson.toString
      Http(requestWithBody OK as.String).option.apply() should not be None
      plan.sessionManager.session2key.synchronized {
        val session = plan.sessionManager.key2session.get(key)
        session should not be None
        val maybeLoc = plan.sessionManager.session2loc.get(session.get)
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
        plan.sessionManager.session2key.synchronized {
          val session = plan.sessionManager.key2session.get(key)
          session should not be None
          val maybeLoc = plan.sessionManager.session2loc.get(session.get)
          maybeLoc should not be None
          val loc = maybeLoc.get
          loc shouldBe (ip, port)
        }
      }
    }
  }

// persist
  "Registering an existing key in persist" should "succeed" in {
    List(0,1).par.foreach(key => {
      val req = Http(persist_loginUrl.POST OK as.String)
      req.option.apply()
    })

    var keys = List.empty[String]
    // This lock is required because the server is in the same process.
    persist_plan.sessionManager.session2key.synchronized {
      for (key <- persist_plan.sessionManager.key2session.keys)
        keys = key :: keys
    }
    keys.par.foreach(key => {
      val registerJson = write(Register("register", s"8.8.8.8", 30))
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << registerJson.toString
      Http(requestWithBody OK as.String).option.apply() should not be None
      persist_plan.sessionManager.session2key.synchronized {
        val session = persist_plan.sessionManager.key2session.get(key)
        session should not be None
        val maybeLoc = persist_plan.sessionManager.session2loc.get(session.get)
        maybeLoc should not be None
        val loc = maybeLoc.get
        loc shouldBe (s"8.8.8.8", 30)
      }
    })
  }

  "Registering an Unknown key in persist" should "fail" in {
    val req = Http(persist_loginUrl.POST OK as.String)
    var sessionId = req.option.apply() match {
      case Some(res) =>
        parseOpt(res).get.extractOpt[SessionId].get.session_id
      case None => fail()
    }

    var keys = List.empty[String]
    // This lock is required because the server is in the same process.
    persist_plan.sessionManager.session2key.synchronized {
      for (key <- persist_plan.sessionManager.key2session.keys)
        keys = key :: keys
    }
    //キー削除
    val key = persist_plan.sessionManager.session2key.get(sessionId).get
    persist_plan.sessionManager.key2session -= key

    val registerJson = write(Register("register", s"8.8.8.8", 30))
    val requestWithBody = requestAsJson(key, persist_registrationUrl) << registerJson.toString
    val res = Http(requestWithBody).option.apply()
    res match {
      case Some(res) =>
        res.getStatusCode.shouldBe(500)
        res.getResponseBody.shouldBe(s"Failed to register key : ${key}")
      case None =>
        fail()
    }

  }

  "Registering connect zookeeper failed in persist" should "fail" in {
    val req = Http(persist_loginUrl.POST OK as.String)
    val sessionId = req.option.apply() match {
      case Some(res) =>
        parseOpt(res).get.extractOpt[SessionId].get.session_id
      case None => fail()
    }

    var keys = List.empty[String]
    // This lock is required because the server is in the same process.
    persist_plan.sessionManager.session2key.synchronized {
      for (key <- persist_plan.sessionManager.key2session.keys)
        keys = key :: keys
    }
    val key = persist_plan.sessionManager.session2key.get(sessionId).get
    zkServer.stop()

    val registerJson = write(Register("register", s"8.8.8.8", 30))
    val requestWithBody = requestAsJson(key, persist_registrationUrl) << registerJson.toString
    val res = Http(requestWithBody).option.apply()
    res match {
      case Some(res) =>
        println(res.getStatusCode)
        println(res.getResponseBody)
        res.getStatusCode.shouldBe(500)
        res.getResponseBody.shouldBe(s"Failed to register key : ${key}")
      case None =>
        fail()
    }
  }
}
