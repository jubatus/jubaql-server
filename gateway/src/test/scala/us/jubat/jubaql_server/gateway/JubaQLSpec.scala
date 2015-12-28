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

import us.jubat.jubaql_server.gateway.json.SessionId
import us.jubat.jubaql_server.gateway.json.Query
import org.scalatest._
import EitherValues._
import dispatch._
import dispatch.Defaults._
import org.json4s._
import org.json4s.Formats._
import org.json4s.native.Serialization.{read, write}
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._

// We use mock processor in this test, so queries are dummies.

class JubaQLSpec extends FlatSpec with Matchers with ProcessorAndGatewayServer {
  val jubaQlUrl  = :/("localhost", 9877) / "jubaql"

  val persist_jubaQlUrl  = :/("localhost", 9880) / "jubaql"
  val register_persist_jubaQlUrl  = :/("localhost", 9880) / "registration"
  val persist_loginUrl  = :/("localhost", 9880) / "login"

  implicit val formats = DefaultFormats

  def requestAsJson(url : Req = jubaQlUrl) = {
    val request = (url).POST
    request.setContentType("application/json", "UTF-8")
  }

  "Posting jubaql with not a JSON" should "fail" in {
    val request = requestAsJson() << "abc"
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with an empty JSON object" should "fail" in {
    val request = requestAsJson() << "{}"
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql without session_id" should "fail" in {
    val request = requestAsJson() << """{"query": "query"}"""
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with inconsistent data" should "fail" in {
    plan.sessionManager.session2key += ("sessionId" -> null)
    plan.sessionManager.key2session += ("sessionKey" -> "persistSessionId")
    plan.sessionManager.session2loc += ("sessionId" -> ("localhost", 9876))

    val request = requestAsJson(jubaQlUrl) << write(Query("sessionId", "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 500
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql without query" should "fail" in {
    val request = requestAsJson() << f"""{"session_id": "$session%s"}"""
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with unknown session_id" should "fail" in {
    val request = requestAsJson() << write(Query("NOSUCHID", "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 401
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with a valid JSON" should "succeed" in {
    val request = requestAsJson() << write(Query(session, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 200
    result.right.value.getContentType should include("charset=utf-8")
  }

  // persist
  // sesssion Idなし
  "Posting jubaql with unknown session id in persist" should "fail" in {
    println(session)
    val request = requestAsJson(persist_jubaQlUrl) << write(Query(session, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 401
    result.right.value.getContentType should include("charset=utf-8")
  }

  // register yet
  "Posting jubaql with register yet in persist" should "fail" in {
    val connectedSessionId = {
      val req = Http(persist_loginUrl.POST OK as.String)
      req.option.apply.get
    }
    println("first connect : " + connectedSessionId)
    // Register
    val json = parseOpt(connectedSessionId)
    val sessionId = json.get.extractOpt[SessionId].get.session_id

    val request = requestAsJson(persist_jubaQlUrl) << write(Query(sessionId, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 503
    result.right.value.getContentType should include("charset=utf-8")
  }

  // キャッシュからセッション取得
  "Posting jubaql with exist session cache in persist" should "succeed" in {
    persist_plan.sessionManager.session2key += ("persistSessionId" -> "persistSessionKey")
    persist_plan.sessionManager.session2loc += ("persistSessionId" -> ("localhost", 9876))
    val request = requestAsJson(persist_jubaQlUrl) << write(Query("persistSessionId", "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 200
    result.right.value.getContentType should include("charset=utf-8")
  }

  // Zookeeperにセッションあり(キャッシュはなし)
  "Posting jubaql with exist session zookeeper in persist" should "succeed" in {
    val connectedSessionId = {
      val req = Http(persist_loginUrl.POST OK as.String)
      req.option.apply.get
    }
    println("first connect : " + connectedSessionId)
    //Register
    val json = parseOpt(connectedSessionId)
    val sessionId = json.get.extractOpt[SessionId].get.session_id
    val key = persist_plan.sessionManager.session2key.get(sessionId).get
    val registJsonString = """{ "action": "register", "ip": "localhost", "port": 9876 }"""
    Http(register_persist_jubaQlUrl./(key).POST << registJsonString).either.apply() match {
      case Right(resultJson) =>
        println("register success: " + resultJson.getResponseBody)
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
    // キャッシュを削除
    persist_plan.sessionManager.session2key -= sessionId
    persist_plan.sessionManager.key2session -= key
    persist_plan.sessionManager.session2loc -= sessionId

    val request = requestAsJson(persist_jubaQlUrl) << write(Query(sessionId, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 200
    result.right.value.getContentType should include("charset=utf-8")
  }

  // Zookeeperへの接続失敗
  "Posting jubaql with failed connect zookeeper in persist" should "fail" in {
    val connectedSessionId = {
      val req = Http(persist_loginUrl.POST OK as.String)
      req.option.apply.get
    }
    println("first connect : " + connectedSessionId)
    // Register
    val json = parseOpt(connectedSessionId)
    val sessionId = json.get.extractOpt[SessionId].get.session_id
    val key = persist_plan.sessionManager.session2key.get(sessionId).get
    val registJsonString = """{ "action": "register", "ip": "localhost", "port": 9876 }"""
    Http(register_persist_jubaQlUrl./(key).POST << registJsonString).either.apply() match {
      case Right(resultJson) =>
        println("register success: " + resultJson.getResponseBody)
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
    // 試験用にキャッシュ情報を削除
    persist_plan.sessionManager.session2key -= sessionId
    persist_plan.sessionManager.key2session -= key
    persist_plan.sessionManager.session2loc -= sessionId

    // Zookeeper停止
    zkServer.stop()

    val request = requestAsJson(persist_jubaQlUrl) << write(Query(sessionId, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    println(result.right.value.getResponseBody)
    result.right.value.getStatusCode shouldBe 500
    result.right.value.getContentType should include("charset=utf-8")
  }
}
