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

import org.scalatest._
import scala.util._


class SessionManagerSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sessionManager = new SessionManager("gatewayID", new DummyStore)

  "initCache()" should "succeed" in {
    // コンストラクタで起動済みのセッション情報を利用
    sessionManager.session2key.get("dummySession1").get shouldBe("dummyKey1")
    sessionManager.session2key.get("dummySession2").get shouldBe("dummyKey2")
    sessionManager.key2session.get("dummyKey1").get shouldBe("dummySession1")
    sessionManager.key2session.get("dummyKey2").get shouldBe("dummySession2")
    sessionManager.session2loc.get("dummySession1").get shouldBe(("dummyHost1",11111))
    sessionManager.session2loc.get("dummySession2").get shouldBe(("dummyHost2",11112))
  }

  it should "fail" in {
    try {
      new SessionManager("addDeleteListenerFailed", new DummyStore)
      fail()
    } catch {
      case e:Exception => e.getMessage shouldBe("addDeleteListenerFailed")
    }
  }

  "createNewSession()" should "succeed" in {
    //parallel execute
    var resultMap = Map.empty[String, String]
    List(1,2,3,4,5).par.foreach { x =>
      val result = sessionManager.createNewSession()
      result match {
        case Success((id, key)) =>
          id.length should be > 0
          key.length should be > 0
          resultMap += id -> key
        case Failure(t) =>
          t.printStackTrace()
          fail()
      }
    }
    for ((key,value) <- resultMap) {
      sessionManager.session2key.get(key).get shouldBe value
      sessionManager.key2session.get(value).get shouldBe key
    }
  }
  it should "fail" in {
    val sessionManager = new SessionManager("preregisterSessionFailed", new DummyStore)
    val result = sessionManager.createNewSession()
    result match {
      case Success((id, key)) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("preregisterSessionFailed")
        sessionManager.session2key.size shouldBe 2 //dummysession
        sessionManager.key2session.size shouldBe 2 //dummysession
        sessionManager.session2loc.size shouldBe 2 //dummysession
    }
  }

  "getSession() find readyState in cache" should "succeed" in {
    val readyResult = sessionManager.getSession("dummySession1")
    readyResult match {
      case Success(state) if state.isInstanceOf[SessionState.Ready] =>
        state.asInstanceOf[SessionState.Ready].host shouldBe("dummyHost1")
        state.asInstanceOf[SessionState.Ready].port shouldBe(11111)
        state.asInstanceOf[SessionState.Ready].key shouldBe("dummyKey1")
      case Success(state) =>
        println(state)
        fail()
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "getSession() find registeringState in cache" should "succeed" in {
    //register registeringState
    val (id,key) = sessionManager.createNewSession() match {
      case Success((id, key)) => (id, key)
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
    println("--- get registeringState ---")
    val registeringResult = sessionManager.getSession(id)
    registeringResult match {
      case Success(state) if state.isInstanceOf[SessionState.Registering] =>
        state.asInstanceOf[SessionState.Registering].key shouldBe(key)
      case Success(state) =>
        println(state)
        fail()
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "getSession() find inconsistantData in cache" should "succeed" in {
    //register registeringState
    val (id,key) = sessionManager.createNewSession() match {
      case Success((id, key)) => (id, key)
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
    //不整合データ生成
    sessionManager.session2key -= id
    sessionManager.session2key += id -> null
    sessionManager.session2loc += id -> ("inconsistHost", 9999)

    println("--- get inconsitantData ---")
    sessionManager.getSession(id) match {
      case Success(state) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe(s"Inconsistent data. sessionId: ${id}")
    }
  }

  "getSession() find readyState in store" should "succeed" in {
    val readyResult = sessionManager.getSession("ReadySession")
    readyResult match {
      case Success(state) if state.isInstanceOf[SessionState.Ready] =>
        state.asInstanceOf[SessionState.Ready].host shouldBe("readyHost")
        state.asInstanceOf[SessionState.Ready].port shouldBe(12345)
        state.asInstanceOf[SessionState.Ready].key shouldBe("readyKey")
        sessionManager.session2key.get("ReadySession").get shouldBe("readyKey")
        sessionManager.key2session.get("readyKey").get shouldBe("ReadySession")
        sessionManager.session2loc.get("ReadySession").get shouldBe(("readyHost",12345))
      case Success(state) =>
        println(state)
        fail()
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "getSession() find registeringState in store" should "succeed" in {
    val registeringResult = sessionManager.getSession("RegisteringSession")
    registeringResult match {
      case Success(state) if state.isInstanceOf[SessionState.Registering] =>
        state.asInstanceOf[SessionState.Registering].key shouldBe("registeringKey")
        sessionManager.session2key.get("RegisteringSession") shouldBe None
      case Success(state) =>
        println(state)
        fail()
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "getSession() find notfoundState in store" should "succeed" in {
    val notfoundResult = sessionManager.getSession("NotfoundSession")
    notfoundResult match {
      case Success(state) =>
        state shouldBe SessionState.NotFound
        sessionManager.session2key.get("NotfoundSession") shouldBe None
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "getSession() find inconsistantState in store" should "succeed" in {
    val inconsistantResult = sessionManager.getSession("InconsistentSession")
    inconsistantResult match {
      case Success(state) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("Inconsistent data. sessionId: InconsistentSession")
        sessionManager.session2key.get("InconsistentSession") shouldBe None
    }
  }
  "getSession() sessionStore throw Exception" should "fail" in {
    val sessionManager = new SessionManager("getSessionFailed", new DummyStore)
    val result = sessionManager.getSession("NonSession")
    result match {
      case Success(state) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("getSessionFailed")
        sessionManager.session2key.size shouldBe 2 //dummysession
        sessionManager.key2session.size shouldBe 2 //dummysession
        sessionManager.session2loc.size shouldBe 2 //dummysession
    }
  }

  "attachProcessorToSession() exist sessionId" should "succeed" in {
    var resultMap = Map.empty[String, String]
    List(1,2,3,4,5).par.foreach { x =>
      val (id, key) = sessionManager.createNewSession() match {
        case Success((id, key)) => (id, key)
        case Failure(t) =>
          t.printStackTrace()
          fail()
      }
      val result = sessionManager.attachProcessorToSession("dummyHost" + x, x, key)
      result match {
        case Success(sessionId) =>
          sessionId shouldBe id
          sessionManager.session2loc.get(id).get shouldBe ("dummyHost" + x, x)
        case Failure(t) =>
          t.printStackTrace()
          fail()
      }
    }
  }
  "attachProcessorToSession() non exist sessionId" should "fail" in {
    val result = sessionManager.attachProcessorToSession("dummyHost", 9999, "NonKey")
    result match {
      case Success(sessionId) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe(s"non exist sessionId. key: NonKey")
        sessionManager.key2session.get("NonKey") shouldBe None
    }
  }
  "attachProcessorToSession() sessionStore throw Exception" should "fail" in {
    val sessionManager = new SessionManager("registerSessionFailed", new DummyStore)
    val (id, key) = sessionManager.createNewSession() match {
      case Success((id, key)) => (id, key)
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
    val result = sessionManager.attachProcessorToSession("dummyHost", 9999, key)
    result match {
      case Success(sessionId) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("registerSessionFailed")
        sessionManager.session2loc.get(id) shouldBe None //dummysession
    }
  }

  "deleteSession() exist session" should "succeed" in {
    val sessionManager = new SessionManager("test", new DummyStore)
    val result = sessionManager.deleteSessionByKey("dummyKey1")
    result match {
      case Success((id, key)) =>
        id shouldBe("dummySession1")
        key shouldBe("dummyKey1")
        sessionManager.session2key.get("dummySession1") shouldBe None
        sessionManager.key2session.get("dummyKey1") shouldBe None
        sessionManager.session2loc.get("dummySession1") shouldBe None
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "deleteSession() non exist session" should "succeed" in {
    val result = sessionManager.deleteSessionByKey("dummyKey3")
    result match {
      case Success((id, key)) =>
        id shouldBe(null)
        key shouldBe("dummyKey3")
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "deleteSession() sessionStore throw Exception" should "fail" in {
    val sessionManager = new SessionManager("deleteSessionFailed", new DummyStore)
    val result = sessionManager.deleteSessionByKey("dummyKey1")
    result match {
      case Success((id, key)) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("deleteSessionFailed")
        sessionManager.session2key.get("dummySession1") should not be None //don't delete session cache
        sessionManager.key2session.get("dummyKey1") should not be None //don't delete session cache
        sessionManager.session2loc.get("dummySession1") should not be None //don't delete session cache
    }
  }

  "deleteSessionById() exist session" should "succeed" in {
    val sessionManager = new SessionManager("test", new DummyStore)
    val result = sessionManager.deleteSessionById("dummySession1")
    result match {
      case Success((id, key)) =>
        id shouldBe("dummySession1")
        key shouldBe("dummyKey1")
        sessionManager.session2key.get("dummySession1") shouldBe None
        sessionManager.key2session.get("dummyKey1") shouldBe None
        sessionManager.session2loc.get("dummySession1") shouldBe None
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "deleteSessionById() non exist session" should "succeed" in {
    val result = sessionManager.deleteSessionById("dummySession3")
    result match {
      case Success((id, key)) =>
        id shouldBe("dummySession3")
        key shouldBe(null)
      case Failure(t) =>
        t.printStackTrace()
        fail()
    }
  }
  "deleteSessionById() sessionStore throw Exception" should "fail" in {
    val sessionManager = new SessionManager("deleteSessionByIdFailed", new DummyStore)
    val result = sessionManager.deleteSessionById("dummyKey1")
    result match {
      case Success((id, key)) =>
        fail()
      case Failure(t) =>
        t.getMessage shouldBe("deleteSessionByIdFailed")
        sessionManager.session2key.get("dummySession1") should not be None //don't delete session cache
        sessionManager.key2session.get("dummyKey1") should not be None //don't delete session cache
        sessionManager.session2loc.get("dummySession1") should not be None //don't delete session cache
    }
  }

  "deleteFunction() exist session" should "succeed" in {
    val sessionManager = new SessionManager("test", new DummyStore)
    sessionManager.deleteFunction("dummySession1")

    sessionManager.session2key.get("dummySession1") shouldBe None
    sessionManager.key2session.get("dummyKey1") shouldBe None
    sessionManager.session2loc.get("dummySession1") shouldBe None
  }
  "deleteFunction() non exist session" should "succeed" in {
    val sessionManager = new SessionManager("test", new DummyStore)
    sessionManager.deleteFunction("NonSession")
    //check don't delete
    sessionManager.session2key.size shouldBe 2
    sessionManager.key2session.size shouldBe 2
    sessionManager.session2loc.size shouldBe 2
  }

  "lock()" should "succeed" in {
    val lock = sessionManager.lock()
    lock.lockObject should not be None
    sessionManager.unlock(lock)
  }
  it should "fail" in {
    val sessionManager = new SessionManager("lockFailed", new DummyStore)
    try {
      sessionManager.lock()
      fail()
    } catch {
      case e:Exception =>
        e.getMessage shouldBe("lockFailed")
    }
  }

  "unlock() sessionStore throw Exception" should "succeed" in {
    val sessionManager = new SessionManager("unLockFailed", new DummyStore)
    try {
      val lock = sessionManager.lock()
      sessionManager.unlock(lock)
    } catch {
      case e: Exception =>
        fail()
    }
  }

}