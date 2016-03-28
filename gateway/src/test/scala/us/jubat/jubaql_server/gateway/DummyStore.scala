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

import scala.collection.mutable._

class DummyStore extends SessionStore {

  override def getAllSessions(gatewayId: String): Map[String, SessionState] = {
    println(s"call getAllSessions gatewayId= ${gatewayId}")
    if (gatewayId.contains("getAllSessionsFailed")) {
      throw new Exception("getAllSessionsFailed")
    }
    val result = Map.empty[String, SessionState]
    result += "dummySession1" -> SessionState.Ready("dummyHost1",11111,"dummyKey1")
    result += "dummySession2" -> SessionState.Ready("dummyHost2",11112,"dummyKey2")
    result += "dummySession3" -> SessionState.Registering("dummyKey3")
    result += "dummySession4" -> SessionState.Inconsistent
    result += "dummySession5" -> SessionState.NotFound
    result += "dummySession6" -> null
  }

  override def getSession(gatewayId: String, sessionId: String): SessionState = {
    println(s"call getSession : gatewayId= ${gatewayId}, sessionId= ${sessionId}")
    if (gatewayId.contains("getSessionFailed")) {
      throw new Exception("getSessionFailed")
    }
    val notfoundRe = """(.*Notfound.*)""".r
    val inconsistentRe = """(.*Inconsistent.*)""".r
    val registeringRe = """(.*Registering.*)""".r
    val readyRe = """(.*Ready.*)""".r

    sessionId match {
      case notfoundRe(id) => SessionState.NotFound
      case inconsistentRe(id) => SessionState.Inconsistent
      case registeringRe(id) => SessionState.Registering("registeringKey")
      case readyRe(id) => SessionState.Ready("readyHost",12345,"readyKey")
    }
  }
  override def preregisterSession(gatewayId: String, sessionId: String, key: String): Unit = {
    println(s"call preregisterSession: gatewayId= ${gatewayId}, sessionId= ${sessionId}, key= ${key}")
    if (gatewayId.contains("preregisterSessionFailed")) {
      throw new Exception("preregisterSessionFailed")
    }
  }
  override def registerSession(gatewayId: String, sessionId: String, host: String, port: Int, key: String): Unit = {
    println(s"call registerSession: gatewayId= ${gatewayId}, sessionId= ${sessionId}, host= ${host}, port= ${port}, key= ${key}")
    if (gatewayId.contains("registerSessionFailed")) {
      throw new Exception("registerSessionFailed")
    }
  }
  override def deleteSession(gatewayId: String, sessionId: String): Unit = {
    println(s"call deleteSession: gatewayId= ${gatewayId}, sessionId= ${sessionId}")
    if (gatewayId.contains("deleteSessionFailed")) {
      throw new Exception("deleteSessionFailed")
    } else if (gatewayId.contains("deleteSessionByIdFailed")) {
      throw new Exception("deleteSessionByIdFailed")
    }
  }
  override def lock(gatewayId: String): SessionLock = {
    println(s"call lock: gatewayId= ${gatewayId}")
    if (gatewayId.contains("lockFailed")) {
      throw new Exception("lockFailed")
    }
    new SessionLock(gatewayId)
  }
  override def unlock(lock: SessionLock): Unit = {
    if (lock != null ){
      println(s"call unlock: lockObject: ${lock.lockObject}")
    } else {
      println("call unlock: lockObject: null")
    }
    if (lock != null && lock.lockObject != null && lock.lockObject.toString.contains("unLockFailed")) {
      throw new Exception("unLockFailed")
    }
  }
  override def addDeleteListener(gatewayId: String, sessionId: String, deleteFunction: Function1[String, Unit] = null): Unit = {
    println(s"call addDeleteListener: gatewayId= ${gatewayId}, sessionId= ${sessionId}")
    if (gatewayId.contains("addDeleteListenerFailed")) {
      throw new Exception("addDeleteListenerFailed")
    }
  }

  override def registerGateway(gatewayId: String): Unit = {
    println(s"call registerGateway: gatewayId= ${gatewayId}")
    if (gatewayId.contains("registerGatewayFailed")) {
      throw new Exception("registerGatewayFailed")
    }
  }

  override def close(): Unit = {
    println(s"call close")
  }
}