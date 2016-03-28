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
import java.util.concurrent.locks.ReentrantLock

class ZookeeperStoreSpec extends FlatSpec with Matchers with ZookeeperServer {
  val zkStore = new ZookeeperStore()

  var res: String = ""
  var res2: String = ""

  "getAllSessions() NoNode" should "succeed" in {
    val result = zkStore.getAllSessions("NoNode")
    result.size shouldBe (0)
  }

  "getAllSessions() Empty" should "succeed" in {
    zkStore.registerGateway("getAllSessions")
    val result = zkStore.getAllSessions("getAllSessions")
    result.size shouldBe (0)
  }

  "getAllSessions() Size=2" should "succeed" in {
    zkStore.registerGateway("getAllSessions")
    // set ReadySession
    zkStore.preregisterSession("getAllSessions", "sessionID1", "DummyKey1")
    zkStore.registerSession("getAllSessions", "sessionID1", "DummyHost1", 9999, "DummyKey1")
    // set RegisteringSession
    zkStore.preregisterSession("getAllSessions", "sessionID2", "DummyKey2")

    val result = zkStore.getAllSessions("getAllSessions")
    result.size shouldBe (2)
    result.get("sessionID1") should not be None
    result.get("sessionID1").get.isInstanceOf[SessionState.Ready] shouldBe (true)
    val getSession1 = result.get("sessionID1").get.asInstanceOf[SessionState.Ready]
    getSession1.host shouldBe "DummyHost1"
    getSession1.port shouldBe 9999
    getSession1.key shouldBe "DummyKey1"
    result.get("sessionID2") should not be None
    result.get("sessionID2").get.isInstanceOf[SessionState.Registering] shouldBe (true)
    val getSession2 = result.get("sessionID2").get.asInstanceOf[SessionState.Registering]
    getSession2.key shouldBe "DummyKey2"
  }

  "getAllSessions() stop zookeeper" should "fail" in {
    zkStore.registerGateway("getAllSessions")
    zkServer.stop()
    try {
      zkStore.getAllSessions("getAllSessions")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to get all session.")
    }
  }

  "getSession() non exist SessionNode" should "succeed" in {
    zkStore.registerGateway("getSession")
    val result = zkStore.getSession("getSession", "NoSession")
    result shouldBe (SessionState.NotFound)
  }

  "getSession() exist SessionNode" should "succeed" in {
    zkStore.registerGateway("getSession")
    // set ReadySession
    zkStore.preregisterSession("getSession", "sessionID1", "DummyKey1")
    zkStore.registerSession("getSession", "sessionID1", "DummyHost1", 9999, "DummyKey1")
    // set RegisteringSession
    zkStore.preregisterSession("getSession", "sessionID2", "DummyKey2")
    // set InconsistantSession
    zkStore.zookeeperClient.create().forPath(s"${zkStore.zkSessionPath}/getSession/sessionID3", s"""{"host":"DummyHost3", "port": 9999}""".getBytes("UTF-8"))

    val result1 = zkStore.getSession("getSession", "sessionID1")
    val getSession1 = result1.asInstanceOf[SessionState.Ready]
    getSession1.host shouldBe "DummyHost1"
    getSession1.port shouldBe 9999
    getSession1.key shouldBe "DummyKey1"
    val result2 = zkStore.getSession("getSession", "sessionID2")
    val getSession2 = result2.asInstanceOf[SessionState.Registering]
    getSession2.key shouldBe "DummyKey2"
    val result3 = zkStore.getSession("getSession", "sessionID3")
    result3 shouldBe (SessionState.Inconsistent)
    val result4 = zkStore.getSession("getSession", "sessionID4")
    result4 shouldBe (SessionState.NotFound)
  }

  "getSession() illegal data" should "fail" in {
    zkStore.registerGateway("getSession")
    zkStore.zookeeperClient.create().forPath(s"${zkStore.zkSessionPath}/getSession/IllegalData", s"""{"host":"DummyHost", "port": "number", "key": "DummyKey"}""".getBytes("UTF-8"))

    try {
      zkStore.getSession("getSession", "IllegalData")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to get session. sessionId: IllegalData")
    }
  }

  "preregisterSession() non exist SessionNode" should "succeed" in {
    zkStore.registerGateway("preregisterSession")
    zkStore.preregisterSession("preregisterSession", "sessionID1", "DummyKey")
    val result = zkStore.getSession("preregisterSession", "sessionID1")
    result.isInstanceOf[SessionState.Registering] shouldBe (true)
    val session = result.asInstanceOf[SessionState.Registering]
    session.key shouldBe ("DummyKey")
  }

  "preregisterSession() already exist SessionNode" should "fail" in {
    zkStore.registerGateway("preregisterSession")
    zkStore.preregisterSession("preregisterSession", "sessionID2", "DummyKey")
    try {
      zkStore.preregisterSession("preregisterSession", "sessionID2", "DummyKey2")
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to pre-register session. sessionId: sessionID2")
        // 先に登録したデータに影響がないことを確認
        val result = zkStore.getSession("preregisterSession", "sessionID2")
        result.isInstanceOf[SessionState.Registering] shouldBe (true)
        val session = result.asInstanceOf[SessionState.Registering]
        session.key shouldBe ("DummyKey")
    }
  }

  "preregisterSession() stop zookeeper" should "fail" in {
    zkStore.registerGateway("preregisterSession")
    zkServer.stop()
    try {
      zkStore.preregisterSession("preregisterSession", "sessionID3", "DummyKey")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to pre-register session. sessionId: sessionID3")
    }
  }

  "registerSession() non exist SessionNode" should "fail" in {
    zkStore.registerGateway("registerSession")
    try {
      zkStore.registerSession("registerSession", "sessionID1", "DummyHost", 9999, "DummyKey")
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to register session. sessionId: sessionID1")
        val result = zkStore.getSession("registerSession", "sessionID1")
        result shouldBe SessionState.NotFound
    }
  }

  "registerSession() exist SessionNode" should "fail" in {
    zkStore.registerGateway("registerSession")
    zkStore.preregisterSession("registerSession", "sessionID2", "DummyKey")
    zkStore.registerSession("registerSession", "sessionID2", "DummyHost2", 9999, "DummyKey2")
    val result = zkStore.getSession("registerSession", "sessionID2")
    val getSession = result.asInstanceOf[SessionState.Ready]
    getSession.host shouldBe "DummyHost2"
    getSession.port shouldBe 9999
    getSession.key shouldBe "DummyKey2"
  }

  "registerSession() alreadyRegistered SessionNode" should "fail" in {
    zkStore.registerGateway("registerSession")
    zkStore.preregisterSession("registerSession", "sessionID3", "DummyKey")
    zkStore.registerSession("registerSession", "sessionID3", "DummyHost3", 9999, "DummyKey3")

    try {
      zkStore.registerSession("registerSession", "sessionID3", "DummyHost4", 9999, "DummyKey4")
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to register session. sessionId: sessionID3")
        val result = zkStore.getSession("registerSession", "sessionID3")
        val getSession = result.asInstanceOf[SessionState.Ready]
        getSession.host shouldBe "DummyHost3"
        getSession.port shouldBe 9999
        getSession.key shouldBe "DummyKey3"
    }
  }

  "registerSession() stop zookeeper" should "fail" in {
    zkStore.registerGateway("registerSession")
    zkStore.preregisterSession("registerSession", "sessionID4", "DummyKey")

    zkServer.stop
    try {
      zkStore.registerSession("registerSession", "sessionID4", "DummyHost4", 9999, "DummyKey4")
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to register session. sessionId: sessionID4")
    }
  }

  "deleteSession() non exist SessionNode" should "succeed" in {
    zkStore.registerGateway("deleteSession")
    try {
      zkStore.deleteSession("deleteSession", "sessionID1")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        fail
    }
  }

  "deleteSession() exist Registering SessionNode" should "succeed" in {
    zkStore.registerGateway("deleteSession")
    zkStore.preregisterSession("deleteSession", "sessionID1", "DummyKey1")

    zkStore.deleteSession("deleteSession", "sessionID1")
    val result = zkStore.getSession("deleteSession", "sessionID1")
    result shouldBe (SessionState.NotFound)
  }

  "deleteSession() exist Registered SessionNode" should "succeed" in {
    zkStore.registerGateway("deleteSession")
    zkStore.preregisterSession("deleteSession", "sessionID2", "DummyKey2")
    zkStore.registerSession("deleteSession", "sessionID2", "DummyHost2", 9999, "DummyKey2")

    zkStore.deleteSession("deleteSession", "sessionID2")
    val result = zkStore.getSession("deleteSession", "sessionID2")
    result shouldBe (SessionState.NotFound)
  }

  "deleteSession() stop zookeeper" should "fail" in {
    zkStore.registerGateway("deleteSession")
    zkStore.preregisterSession("deleteSession", "sessionID3", "DummyKey3")
    zkServer.stop

    try {
      zkStore.deleteSession("deleteSession", "sessionID3")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to delete session. sessionId: sessionID3")
    }
  }

  "lock() non exist SessionNode" should "succeed" in {
    val result = zkStore.lock("lock")
    result should not be null
    zkStore.unlock(result)
  }

  "lock() exist SessionNode" should "succeed" in {
    zkStore.registerGateway("lock")
    val result = zkStore.lock("lock")
    result should not be null
    zkStore.unlock(result)
  }

  //  "lock() mulitlock timeout" should "succeed" in {
  //    // 多重ロック＋ロックタイムアウトの確認
  //    // lockTimeout時間待ち合わせるため、実行する場合は、定数を変更する。
  //    zkStore.registerGateway("lock")
  //      val result = zkStore.lock("lock")
  //      result should not be null
  //      val result2 = zkStore.lock("lock")
  //      result2 should not be null
  //      zkStore.unlock(result2)
  //  }

  "lock() stop zookeeper" should "fail" in {
    zkStore.registerGateway("lock")
    zkServer.stop
    try {
      zkStore.lock("lock")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to create lock object. gatewayId: lock")
    }
  }

  "unlock() sessionLock null" should "fail" in {
    zkStore.registerGateway("unlock")
    try {
      zkStore.unlock(null)
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to unlock. session lock: null")
    }
  }

  "unlock() lockObject null" should "fail" in {
    zkStore.registerGateway("unlock")
    val sessionLock = new SessionLock(null)
    try {
      zkStore.unlock(sessionLock)
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to unlock. lock object: null")
    }
  }
  "unlock() illegal parameter" should "fail" in {
    zkStore.registerGateway("unlock")
    val illegalLockObj = new ReentrantLock()
    val sessionLock = new SessionLock(illegalLockObj)
    try {
      zkStore.unlock(sessionLock)
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to unlock. illegal lock object: class java.util.concurrent.locks.ReentrantLock")
    }
  }

  "unlock() stop zookeeper" should "fail" in {
    zkStore.registerGateway("unlock")
    val lockObj = zkStore.lock("unlock")
    zkServer.stop
    try {
      zkStore.unlock(lockObj)
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to unlock")
    }
  }

  "addDeleteListener() add Session" should "succeed" in {
    res = ""
    zkStore.registerGateway("addDeleteListener")
    zkStore.preregisterSession("addDeleteListener", "sessionID1", "dummyKey1")

    zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
        fail()
      })
    zkStore.deleteSession("addDeleteListener", "sessionID1")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("sessionID1")
  }

  "addDeleteListener() deleted Session" should "succeed" in {
    res = ""
    zkStore.registerGateway("addDeleteListener")
    zkStore.preregisterSession("addDeleteListener", "sessionID1", "dummyKey1")
    zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "sessionID1")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("sessionID1")
    zkStore.deleteSession("addDeleteListener", "sessionID1")
  }

  "addDeleteListener() add Multi Session" should "succeed" in {
    res = ""
    res2 = ""
    zkStore.registerGateway("addDeleteListener")
    zkStore.preregisterSession("addDeleteListener", "sessionID1", "dummyKey1")
    zkStore.preregisterSession("addDeleteListener", "sessionID2", "dummyKey2")
    zkStore.registerSession("addDeleteListener", "sessionID2", "dummyHost2", 9999, "dummyKey2")

    zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function1. sessionId: ${str}")
      })
    zkStore.addDeleteListener("addDeleteListener", "sessionID2", (str: String) =>
      {
        res2 = str
        println(s"call Dummy Delete Function2. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "sessionID1")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("sessionID1")
    res2 shouldBe ("")
  }

  "addDeleteListener() Re add Session" should "succeed" in {
    zkStore.registerGateway("addDeleteListener")
    zkStore.preregisterSession("addDeleteListener", "sessionID1", "dummyKey1")
    zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "sessionID1")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("sessionID1")

    //Re: add deleteListener
    zkStore.preregisterSession("addDeleteListener", "sessionID1", "dummyKey1")
    res = ""
    zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "sessionID1")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("sessionID1")
  }

  "addDeleteListener() exclude SessionId" should "succeed" in {
    res = ""
    zkStore.registerGateway("addDeleteListener")
    zkStore.zookeeperClient.create().forPath(s"${zkStore.zkSessionPath}/addDeleteListener/locks", """""".getBytes("UTF-8"))
    zkStore.addDeleteListener("addDeleteListener", "locks", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "locks")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("")

    zkStore.zookeeperClient.create().forPath(s"${zkStore.zkSessionPath}/addDeleteListener/leases", """""".getBytes("UTF-8"))
    res = ""
    zkStore.addDeleteListener("addDeleteListener", "leases", (str: String) =>
      {
        res = str
        println(s"call Dummy Delete Function. sessionId: ${str}")
      })
    zkStore.deleteSession("addDeleteListener", "leases")
    //削除イベント処理待ち
    Thread.sleep(1000)
    res shouldBe ("")
  }

  "addDeleteListener() stop zookeeper" should "fail" in {
    zkStore.registerGateway("addDeleteListener")
    zkServer.stop
    try {
      val str = "hoge"
      zkStore.addDeleteListener("addDeleteListener", "sessionID1", (str: String) => { println(s"Dummy Delete Function:${str}") })
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to add delete listener. sessionId: sessionID1")
    }
  }

  "registerGateway() stop zookeeper" should "fail" in {
    zkServer.stop
    try {
      zkStore.registerGateway("registerGateway")
      fail
    } catch {
      case e: Exception =>
        e.getMessage shouldBe ("failed to registerGateway. gatewayId: registerGateway")
    }
  }

}