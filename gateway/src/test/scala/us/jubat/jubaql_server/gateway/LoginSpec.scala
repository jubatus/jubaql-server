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
import org.scalatest._
import dispatch._
import dispatch.Defaults._
import org.json4s._
import org.json4s.Formats._
import org.json4s.native.Serialization.{read, write}
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext

class LoginSpec extends FlatSpec with Matchers with GatewayServer with BeforeAndAfter {

  implicit val formats = DefaultFormats

  before {
    zkServer.restart
  }

  val url = :/("localhost", 9877) / "login"
  val regist_url = :/("localhost", 9877) / "registration"
  val pro_url = :/("localhost", 9878) / "login"
  val dev_url = :/("localhost", 9879) / "login"
  val persist_url = :/("localhost", 9880) / "login"
  val persist_regist_url = :/("localhost", 9880) / "registration"

  "POST to /login" should "return something" in {
    val req = Http(url.POST OK as.String)
    req.option.apply() should not be None
  }

  "POST to /login" should "return a JSON" in {
    val req = Http(url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
  }

  "POST to /login" should "return a JSON which contains session_id" in {
    val req = Http(url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
    val maybeSessionId = maybeJson.get.extractOpt[SessionId]
    maybeSessionId should not be None
    maybeSessionId.get.session_id.length should be > 0

  }

  "POST to /login reconnect" should "not exist sessionId. return Error Response(unknown sessioId)" in {
    //
    val sessionId = "testSessionId"
    val payloadData = ("session_id" -> sessionId)
    val json: String = compact(render(payloadData))

    val req = Http(url.POST << json)// OK as.String)
    req.either.apply() match {
      case Right(response) =>
        response.getStatusCode shouldEqual(401)
        response.getResponseBody shouldBe("Unknown session_id")
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
  }

  "POST to /login reconnect" should "session inconsistency. return Error Response(inconsist data)" in {
    // 初回接続
    val connectedSessionId = connection(url)

    println("first connect : " + connectedSessionId)
    //session情報を不整合状態に書き換え
    plan.sessionManager.session2key -= connectedSessionId
    plan.sessionManager.session2key += connectedSessionId -> null
    plan.sessionManager.session2loc += connectedSessionId -> ("dummyHost", 9999)

    //接続済sessionIdを利用した接続
    val payloadData = ("session_id" -> connectedSessionId)
    val json: String = compact(render(payloadData))

    val req = Http(url.POST << json)
    req.either.apply() match {
      case Right(response) =>
        println(response.getResponseBody)
        response.getStatusCode shouldEqual(500)
        response.getResponseBody shouldBe("Failed to get session")
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
  }

  "POST to /login reconnect" should "exist sessionId, but registered yet. return Error Response(registered yet)" in {
    // 初回接続
    val connectedSessionId = connection(url)

    println("first connect : " + connectedSessionId)
    //接続済sessionIdを利用した接続
    val payloadData = ("session_id" -> connectedSessionId)
    val json: String = compact(render(payloadData))

    val req = Http(url.POST << json)
    req.either.apply() match {
      case Right(response) =>
        response.getStatusCode shouldEqual(503)
        response.getResponseBody shouldBe("This session has not been registered. Wait a second.")
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
  }

  // RunMode.testで実施(非永続化)
  "POST to /login reconnect" should "success reconnect" in {
    // 初回接続
    val connectedSessionId = connection(url)

    println("first connect : " + connectedSessionId)

    //疑似Registration
    registration(regist_url,plan,connectedSessionId)

    //接続済sessionIdを利用した接続
    reconnection(url,connectedSessionId)
  }

  // 非永続化における再接続(接続失敗ケース)
  "POST to /login reconnect" should "reboot gateaway" in {
    // 初回接続
    val connectedSessionId = connection(url)

    println("first connect : " + connectedSessionId)

    //疑似Registration
    registration(regist_url,plan,connectedSessionId)

    // Gateway再起動
    server.stop()
    reserver.start()

    // 接続済sessionIdを利用した接続
    val payloadData = ("session_id" -> connectedSessionId)
    val json: String = compact(render(payloadData))

    val req = Http(url.POST << json)

    // 永続化していないため、エラーレスポンス返却
    req.option.apply.get.getStatusCode shouldBe(401)
    req.option.apply.get.getResponseBody shouldBe("Unknown session_id")
    reserver.stop()
  }

  // 永続化確認
  "reconnect in persist" should "save session to zookeeper" in {
    // 初回接続
    val connectedSessionId = connection(persist_url)

    println("first connect : " + connectedSessionId)

    //疑似Registration
    registration(persist_regist_url, persist_plan, connectedSessionId)

    //接続済sessionIdを利用した接続
    reconnection(persist_url, connectedSessionId)

    // キャッシュへのセッション情報格納を確認
    val cacheKey = persist_plan.sessionManager.session2key.get(connectedSessionId)
    val cacheSessionId = persist_plan.sessionManager.key2session.get(cacheKey.get)
    val cacheLocation = persist_plan.sessionManager.session2loc.get(connectedSessionId)
    cacheKey.get.length() should be > 0
    cacheSessionId.get shouldBe (connectedSessionId)
    cacheLocation.get shouldBe (("dummyhost", 1111))

    // zkへのセッション情報格納を確認
    val storeSession: SessionState = persist_plan.sessionManager.getSessionFromStore(connectedSessionId)
    storeSession shouldBe SessionState.Ready("dummyhost", 1111, cacheKey.get)
    val completeSession = storeSession.asInstanceOf[SessionState.Ready]
    completeSession.host shouldBe "dummyhost"
    completeSession.port shouldBe 1111
    completeSession.key shouldBe cacheKey.get
  }

   // Gatewayサーバ再起動によるセッション接続
  "reconnect in persist" should "reboot gateway" in {
    // 初回接続
    val firstplan = new GatewayPlan("example.com", 1880,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:8880", true)
    val firstserver = unfiltered.netty.Server.http(8880).plan(firstplan)
    firstserver.start()

    val url1 = :/("localhost", 8880) / "login"
    val regist_url1 = :/("localhost", 8880) / "registration"
    val connectedSessionId = connection(url1)

    println("first connect : " + connectedSessionId)

    // 疑似Registration
    registration(regist_url1, firstplan, connectedSessionId)

    // Gateway停止
    firstserver.stop()

    // Gateway再起動
    val secondplan = new GatewayPlan("example.com", 1880,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", "localhost:8880", true)
    val secondserver = unfiltered.netty.Server.http(8880).plan(secondplan)
    secondserver.start()

    // 接続済sessionIdを利用した接続
    reconnection(url1, connectedSessionId)

    // キャッシュへのセッション情報格納を確認
    val cacheKey = secondplan.sessionManager.session2key.get(connectedSessionId)
    val cacheSessionId = secondplan.sessionManager.key2session.get(cacheKey.get)
    val cacheLocation = secondplan.sessionManager.session2loc.get(connectedSessionId)
    cacheKey.get.length() should be > 0
    cacheSessionId.get shouldBe (connectedSessionId)
    cacheLocation.get shouldBe (("dummyhost", 1111))

    // zookeeperへのセッション情報格納を確認
    val storeSession: SessionState = secondplan.sessionManager.getSessionFromStore(connectedSessionId)
    storeSession.isInstanceOf[SessionState.Ready].shouldBe(true)
    val completeSession = storeSession.asInstanceOf[SessionState.Ready]
    completeSession.host shouldBe "dummyhost"
    completeSession.port shouldBe 1111
    completeSession.key shouldBe cacheKey.get
    secondserver.stop()
    firstplan.close()
    secondplan.close()
  }

  "zookeeper connection failed" should "throw Exception" in {
    zkServer.stop()
    try {
      new GatewayPlan("example.com", 1237,
        Array(), RunMode.Test,
        sparkDistribution = "",
        fatjar = "src/test/resources/processor-logfile.jar",
        checkpointDir = "file:///tmp/spark", "localhost:9877", true)
      fail()
    } catch {
      case e: Exception =>
        e.getMessage().shouldBe("failed to connected zookeeper")
    }
  }

  "POST to /login stop zookeeper" should "failed login" in {
    zkServer.stop()
    val req = Http(persist_url.POST)
    req.option.apply.get.getStatusCode.shouldBe(500)
    req.option.apply.get.getResponseBody.shouldBe("Failed to create session")
  }

  "POST to /login stop zookeeper(reconnect)" should "failed login" in {
    zkServer.stop()
    val payloadData = ("session_id" -> "testSessionId")
    val json: String = compact(render(payloadData))

    val req = Http(persist_url.POST << json)
    req.option.apply.get.getStatusCode.shouldBe(500)
    req.option.apply.get.getResponseBody.shouldBe("Failed to get session")
  }

  "POST to /login clustering" should "succeed" in {
    val url1 = :/("localhost", 9890) / "login"
    val regist_url1 = :/("localhost", 9890) / "registration"
    val url2 = :/("localhost", 9891) / "login"
    val regist_url2 = :/("localhost", 9891) / "registration"
    val clustername = "gwCluster"

    val node1_plan = new GatewayPlan("example.com", 1290,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", clustername, true)
    val server1 = unfiltered.netty.Server.http(9890).plan(node1_plan)
    val node2_plan = new GatewayPlan("example.com", 1291,
      Array(), RunMode.Test,
      sparkDistribution = "",
      fatjar = "src/test/resources/processor-logfile.jar",
      checkpointDir = "file:///tmp/spark", clustername, true)
    val server2 = unfiltered.netty.Server.http(9891).plan(node1_plan)
    server1.start()
    server2.start()

    val connectedSessionId = connection(url1)
    registration(regist_url1, node1_plan, connectedSessionId)
    reconnection(url2, connectedSessionId)
    val registeringId = connection(url2)
    reconnectionNJ(url1, registeringId) shouldBe "This session has not been registered. Wait a second."

  }
// ---------------------
  private def connection(url: Req): String = {
    val req = Http(url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
    val maybeSessionId = maybeJson.get.extractOpt[SessionId]
    maybeSessionId should not be None
    //セッションIDの返却チェック
    maybeSessionId.get.session_id.length should be > 0
    maybeSessionId.get.session_id
  }

  private def connectionNJ(url: Req): String = {
    val req = Http(url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    if (maybeJson == None) {
      ""
    } else {
      val maybeSessionId = maybeJson.get.extractOpt[SessionId]
      if (maybeSessionId == None) {
        ""
      } else {
        maybeSessionId.get.session_id
      }
    }
  }

  private def reconnectionNJ(url: Req, sessionId: String):String = {
    val payloadData = ("session_id" -> sessionId)
    val json: String = compact(render(payloadData))
    val req = Http(url.POST << json)
    req.option.apply match {
      case Some(res) =>
        println(res.getResponseBody)
        res.getResponseBody
      case None =>
        ""
    }
  }

  private def reconnection(url:Req, sessionId: String) = {
    val payloadData = ("session_id" -> sessionId)
    val json: String = compact(render(payloadData))

    val req = Http(url.POST << json)

    val returnedString = req.option.apply.get.getResponseBody
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
    val maybeSessionId = maybeJson.get.extractOpt[SessionId]
    maybeSessionId should not be None
    // セッションIDの返却チェック
    maybeSessionId.get.session_id shouldBe (sessionId)
  }

  private def registration(url: Req, plan: GatewayPlan, sessionId: String): Unit = {
    val key = plan.sessionManager.session2key(sessionId)
    val registJsonString = """{ "action": "register", "ip": "dummyhost", "port": 1111 }"""
    Http(url./(key).POST << registJsonString).either.apply() match {
      case Right(resultJson) =>
        println(resultJson.getResponseBody)
      case Left(t) =>
        t.printStackTrace()
        fail(t)
    }
  }
}
