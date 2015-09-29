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

  val persist_loginUrl  = :/("localhost", 9880) / "login"
  val persist_registrationUrl = :/("localhost", 9880) / "registration"

  def requestAsJson(key: String, url: Req = registrationUrl) = {
    val request = (url / key).POST
    request.setContentType("application/json", "UTF-8")
  }

  before {
    zkServer.restart

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

    //persist gateway
    // login twice
    for (i <- 0 until 2) {
      val req = Http(persist_loginUrl.POST OK as.String)
      req.option.apply()
    }

    // register all keys
    for (key <- persist_keys) {
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << registerJson
      Http(requestWithBody OK as.String).option.apply()
    }
  }

  after {
    // unregister all keys
    for (key <- keys) {
      val requestWithBody = requestAsJson(key) << unregisterJson
      Http(requestWithBody OK as.String).option.apply()
    }

    for (key <- persist_keys) {
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << unregisterJson
      Http(requestWithBody OK as.String).option.apply()
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

  def persist_keys = {
    var keys = List.empty[String]
    // This lock is required because the server is in the same process.
    persist_plan.sessionManager.session2key.synchronized {
      for (key <- persist_plan.sessionManager.key2session.keys)
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
      plan.sessionManager.session2key.synchronized {
        sessionId = plan.sessionManager.key2session.get(key).get
      }
      val requestWithBody = requestAsJson(key) << unregisterJson
      Http(requestWithBody OK as.String).option.apply() should not be None
      plan.sessionManager.session2key.synchronized {
        plan.sessionManager.session2key.get(sessionId) shouldBe None
        plan.sessionManager.key2session.get(key) shouldBe None
        plan.sessionManager.session2loc.get(sessionId) shouldBe None
      }
    }
  }

  // persist
  "Unregistering an existing key in persist" should "succeed" in {
    persist_keys.par.foreach(key => {
      var sessionId = ""
      persist_plan.sessionManager.session2key.synchronized {
        sessionId = persist_plan.sessionManager.key2session.get(key).get
      }
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << unregisterJson
      Http(requestWithBody OK as.String).option.apply() should not be None
      persist_plan.sessionManager.session2key.synchronized {
        persist_plan.sessionManager.session2key.get(sessionId) shouldBe None
        persist_plan.sessionManager.key2session.get(key) shouldBe None
        persist_plan.sessionManager.session2loc.get(sessionId) shouldBe None
        persist_plan.sessionManager.getSessionFromStore(sessionId) shouldBe SessionState.NotFound
      }
    })
  }

  "Unregistering an already delete key in persist" should "succeed" in {
    persist_keys.par.foreach(key => {
      var sessionId = ""
      persist_plan.sessionManager.session2key.synchronized {
        sessionId = persist_plan.sessionManager.key2session.get(key).get
      }
      println("sessionId : " + sessionId+ ", key : " + key)
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << unregisterJson
      val result = Http(requestWithBody OK as.String).option.apply()
      result should not be None
      result.get shouldBe "Successfully unregistered"
      persist_plan.sessionManager.session2key.synchronized {
        persist_plan.sessionManager.session2key.get(sessionId) shouldBe None
        persist_plan.sessionManager.key2session.get(key) shouldBe None
        persist_plan.sessionManager.session2loc.get(sessionId) shouldBe None
        persist_plan.sessionManager.getSessionFromStore(sessionId) shouldBe SessionState.NotFound
      }

      val alredyDeleteresult = Http(requestWithBody OK as.String).option.apply()
      alredyDeleteresult should not be None
      alredyDeleteresult.get shouldBe "Successfully unregistered"
      persist_plan.sessionManager.session2key.synchronized {
        persist_plan.sessionManager.session2key.get(sessionId) shouldBe None
        persist_plan.sessionManager.key2session.get(key) shouldBe None
        persist_plan.sessionManager.session2loc.get(sessionId) shouldBe None
        persist_plan.sessionManager.getSessionFromStore(sessionId) shouldBe SessionState.NotFound
      }

    })
  }

  "Unregistering connect zookeeper failed in persist" should "fail" in {
    zkServer.stop
    persist_keys.par.foreach(key => {
      var sessionId = ""
      persist_plan.sessionManager.session2key.synchronized {
        sessionId = persist_plan.sessionManager.key2session.get(key).get
      }
      val requestWithBody = requestAsJson(key, persist_registrationUrl) << unregisterJson
      val result = Http(requestWithBody).either.apply()
      result match {
        case Left(t) =>
          println("hogehoge---------------------------: " + t.getMessage)
          fail()
        case Right(res) =>
          res.getResponseBody shouldBe s"Failed to unregister key : ${key}"
          persist_plan.sessionManager.session2key.synchronized {
            persist_plan.sessionManager.session2key.get(sessionId) should not be None
            persist_plan.sessionManager.key2session.get(key) should not be None
            persist_plan.sessionManager.session2loc.get(sessionId) should not be None
          }
      }
    })
    zkServer.restart
  }

  "Unregistering an existing key in JubaQLGateway Cluster" should "succeed" in {

    println("--- gateway cluster test setting ---")
    val gateway1_loginUrl  = :/("localhost", 9980) / "login"
    val gateway2_loginUrl  = :/("localhost", 9981) / "login"
    val gateway1_registrationUrl = :/("localhost", 9980) / "registration"
    val gateway2_registrationUrl = :/("localhost", 9981) / "registration"

    //gateway startup
    val gateway1_plan = new GatewayPlan("example.com", 2345,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "gateway_cluster", true)
    val gateway1_server = unfiltered.netty.Server.http(9980).plan(gateway1_plan)
    gateway1_server.start

    // login
    for (i <- 0 until 2) {
      val req = Http(gateway1_loginUrl.POST OK as.String)
      req.option.apply()
    }
    // get key
    var keys = List.empty[String]
    gateway1_plan.sessionManager.session2key.synchronized {
      for (key <- gateway1_plan.sessionManager.key2session.keys)
        keys = key :: keys
    }
    // register all keys
    for (key <- keys) {
      val requestWithBody = requestAsJson(key, gateway1_registrationUrl) << registerJson
      val res = Http(requestWithBody OK as.String).option.apply()
    }

    //gateway1でregistrationが完了した時点で同クラスタに所属するgateway2を起動
    val gateway2_plan = new GatewayPlan("example.com", 2346,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "gateway_cluster", true)
    val gateway2_server = unfiltered.netty.Server.http(9981).plan(gateway2_plan)
    gateway2_server.start

    keys.par.foreach(key => {
      var sessionId = ""
      gateway1_plan.sessionManager.session2key.synchronized {
        sessionId = gateway1_plan.sessionManager.key2session.get(key).get
      }
      gateway1_plan.sessionManager.session2key.synchronized {
        gateway1_plan.sessionManager.session2key.get(sessionId) should not be None
        gateway1_plan.sessionManager.key2session.get(key) should not be None
        gateway1_plan.sessionManager.session2loc.get(sessionId) should not be None
        gateway1_plan.sessionManager.getSessionFromStore(sessionId).isInstanceOf[SessionState.Ready] shouldBe true
      }
      gateway2_plan.sessionManager.session2key.synchronized {
        gateway2_plan.sessionManager.session2key.get(sessionId) should not be None
        gateway2_plan.sessionManager.key2session.get(key) should not be None
        gateway2_plan.sessionManager.session2loc.get(sessionId) should not be None
        gateway2_plan.sessionManager.getSessionFromStore(sessionId).isInstanceOf[SessionState.Ready] shouldBe true
      }
    })
    println("--- unregistring ---")
    keys.par.foreach(key => {
      var sessionId = ""
      gateway1_plan.sessionManager.session2key.synchronized {
        sessionId = gateway1_plan.sessionManager.key2session.get(key).get
      }
      // セッション情報を登録したgateway以外でunregeister実行
      val requestWithBody = requestAsJson(key, gateway2_registrationUrl) << unregisterJson
      Http(requestWithBody OK as.String).option.apply() should not be None
      // 同クラスタのすべてのgatewayから該当セッション情報が削除されることを確認
      gateway1_plan.sessionManager.session2key.synchronized {
        gateway1_plan.sessionManager.session2key.get(sessionId) shouldBe None
        gateway1_plan.sessionManager.key2session.get(key) shouldBe None
        gateway1_plan.sessionManager.session2loc.get(sessionId) shouldBe None
        gateway1_plan.sessionManager.getSessionFromStore(sessionId) shouldBe SessionState.NotFound
      }
      gateway2_plan.sessionManager.session2key.synchronized {
        gateway2_plan.sessionManager.session2key.get(sessionId) shouldBe None
        gateway2_plan.sessionManager.key2session.get(key) shouldBe None
        gateway2_plan.sessionManager.session2loc.get(sessionId) shouldBe None
        gateway2_plan.sessionManager.getSessionFromStore(sessionId) shouldBe SessionState.NotFound
      }
    })
    println("--- unregistred ---")
    gateway1_server.stop
    gateway2_server.stop
  }
}
