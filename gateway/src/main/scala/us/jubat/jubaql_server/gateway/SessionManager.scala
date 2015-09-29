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

import scala.collection.mutable
import scala.collection.mutable._
import scala.util.Random
import org.json4s._
import org.json4s.native.JsonMethods._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.{Try, Success, Failure}

/**
 * Session Management Class
 */
class SessionManager(gatewayId: String, sessionStore: SessionStore = new NonPersistentStore) extends LazyLogging {
  implicit val formats = DefaultFormats

  // key = sessionId, value = keyInfo
  val session2key: mutable.Map[String, String] = new mutable.HashMap()
  // key = keyInfo, value = sessionId
  // key: identification required for registration / unregistration
  val key2session: mutable.Map[String, String] = new mutable.HashMap()
  // key = sessionId, vale = Processor's connectionInfo(host, port)
  val session2loc: mutable.Map[String, (String, Int)] = new mutable.HashMap()

  // initialize session store(specific processing)
  sessionStore.registerGateway(gatewayId)

  // initialize cache from session store
  initCache()

  // --- session controller method ---
  /**
   * initialize cache from session store
   */
  def initCache(): Unit = {
    val sessionMap = sessionStore.getAllSessions(gatewayId)
    session2key.synchronized {
      for ((sessionId, sessionInfo) <- sessionMap) {
        sessionInfo match {
          case completeSesion: SessionState.Ready =>
            session2key += sessionId -> completeSesion.key
            key2session += completeSesion.key -> sessionId
            session2loc += sessionId -> (completeSesion.host, completeSesion.port)
            sessionStore.addDeleteListener(gatewayId, sessionId, deleteFunction)
          case registeringSession: SessionState.Registering =>
            logger.debug(s"Registering session not add to cache. session: ${sessionId}")
          case _ =>
            logger.info(s"Invalid session. exclude session: ${sessionId}")
        }
      }
    }
  }

  /**
   * create new session
   * @return resultCreateSession(session ID and registration key)
   */
  def createNewSession(): Try[(String, String)] = {
    var lockObj: SessionLock = null
    val result = Try {
      lockObj = lock()
      val (sessionId, key) = createId()
      sessionStore.preregisterSession(gatewayId, sessionId, key)
      sessionStore.addDeleteListener(gatewayId, sessionId, deleteFunction)
      session2key.synchronized {
        session2key += (sessionId -> key)
        key2session += (key -> sessionId)
      }
      (sessionId, key)
    }
    unlock(lockObj)
    result
  }

  /**
   * get Session
   * if not found in cache, contact to session store
   * @param sessionId: sessionId
   * @return SessionInformation (processor's host, processor's port, regstrationKey)
   */
  def getSession(sessionId: String): Try[SessionState] = {
    Try {
      val cacheSession: SessionState = getSessionFromCache(sessionId)
      cacheSession match {
        case completeSession: SessionState.Ready => completeSession
        case registeringSession: SessionState.Registering => registeringSession
        case SessionState.NotFound =>
          // session not found in cache, contact session store.
          val storeSession = sessionStore.getSession(gatewayId, sessionId)
          storeSession match {
            case SessionState.NotFound => storeSession
            case SessionState.Inconsistent =>
              throw new Exception(s"Inconsistent data. sessionId: ${sessionId}")
            case completeSession: SessionState.Ready =>
              val host = completeSession.host
              val port = completeSession.port
              val key = completeSession.key
              sessionStore.addDeleteListener(gatewayId, sessionId, deleteFunction)
              session2key.synchronized {
                session2key += sessionId -> key
                key2session += key -> sessionId
                session2loc += sessionId -> (host, port)
              }
              completeSession
            case registeringSession: SessionState.Registering =>
              registeringSession
          }
      }
    }
  }

  /**
   * get session from cache
   * @param sessionId session ID
   * @return session information
   */
  def getSessionFromCache(sessionId: String): SessionState = {
    session2key.get(sessionId) match {
      case Some(key) =>
        session2loc.get(sessionId) match {
          case Some(loc) =>
            if (key != null) {
              SessionState.Ready(loc._1, loc._2, key)
            } else {
              throw new Exception(s"Inconsistent data. sessionId: ${sessionId}")
            }
          case None =>
            // not yet registered sessionId.
            SessionState.Registering(key)
        }
      case None =>
        SessionState.NotFound
    }
  }

  /**
   * attach Processor Information to session
   * @param host Processor's host
   * @param port Processor's port
   * @param key registration key
   * @return attachResult
   */
  def attachProcessorToSession(host: String, port: Int, key: String): Try[String] = {
    var lockObj: SessionLock = null
    val result = Try {
      val sessionId = key2session.get(key) match {
        case Some(sessionId) => sessionId
        case None => throw new Exception(s"non exist sessionId. key: ${key}")
      }
      lockObj = lock()
      sessionStore.registerSession(gatewayId, sessionId, host, port, key)
      sessionStore.addDeleteListener(gatewayId, sessionId, deleteFunction)
      session2key.synchronized {
        session2loc += (sessionId -> (host, port))
      }
      sessionId
    }
    unlock(lockObj)
    result
  }

  /**
   * delete Session
   * @param key registration key
   * @return deleteResult(sessionId, key)
   */
  def deleteSession(key: String): Try[(String, String)] = {
    var lockObj: SessionLock = null
    val result = Try {
      key2session.get(key) match {
        case Some(sessionId) =>
          lockObj = lock()
          sessionStore.deleteSession(gatewayId, sessionId)
          session2key.synchronized {
            key2session -= key
            session2key -= sessionId
            session2loc -= sessionId
          }
          (sessionId, key)
        case None =>
          logger.debug(s"non exist sessionId. key: ${key}")
          (null, key)
      }
    }
    unlock(lockObj)
    result
  }

  /**
   * gateway session lock
   * @return Lock Object
   */
  def lock(): SessionLock = {
    val lock = sessionStore.lock(gatewayId)
    logger.debug(s"locked: $gatewayId")
    lock
  }

  /**
   * gateway session unlock
   * @param lock Lock Object
   */
  def unlock(lock: SessionLock): Unit = {
    if (lock != null) {
      try {
        sessionStore.unlock(lock)
        logger.debug(s"unlocked: gatewayId = $gatewayId")
      } catch {
        case e: Exception =>
          logger.error("failed to unlock.", e)
      }
    } else {
      logger.debug(s"not lock. gatewayId = $gatewayId")
    }
  }

  /**
   * generate sessionId and key
   * @return (sessionId, registrationKey)
   */
  def createId(): (String, String) = {
    session2key.synchronized {
      var sessionId = ""
      var key = ""

      do {
        sessionId = Alphanumeric.generate(20)
      } while (session2key.get(sessionId) != None)
      do {
        key = Alphanumeric.generate(20)
      } while (key2session.get(key) != None)
      (sessionId, key)
    }
  }

  /**
   * delete session in cache. called by delete listeners.
   * @param sessionId session ID
   */
  def deleteFunction(sessionId: String): Unit = {
    logger.debug("delete session from cache. sessionId = ${sessionId}")
    session2key.synchronized {
      val keyOpt = session2key.get(sessionId)
      keyOpt match {
        case Some(key) => key2session -= key
        case None => //Nothing
      }
      session2key -= sessionId
      session2loc -= sessionId
    }
  }

  /**
   * get session from store
   * @param sessionId session ID
   * @return session information
   */
  def getSessionFromStore(sessionId: String): SessionState = {
    sessionStore.getSession(gatewayId, sessionId)
  }

  /**
   * close
   */
  def close(): Unit = {
    sessionStore.close()
  }
}

/**
 *  An alphanumeric string generator.
 */
object Alphanumeric {
  val random = new Random(new java.security.SecureRandom())
  val chars = "0123456789abcdefghijklmnopqrstuvwxyz"

  /**
   * generate ID
   * @param length generated ID length
   */
  def generate(length: Int): String = {
    val ret = new Array[Char](length)
    this.synchronized {
      for (i <- 0 until ret.length) {
        ret(i) = chars(random.nextInt(chars.length))
      }
    }

    new String(ret)
  }
}

/**
 * Session Store Interface
 */
trait SessionStore {
  implicit val formats = DefaultFormats

  /**
   * get All Session by gatewayId
   * @param gatewayId gateway ID
   * @return SessionInformation Map(key   = sessionId, value = SessionState)
   */
  def getAllSessions(gatewayId: String): Map[String, SessionState]

  /**
   * get Session
   * if not found in cache, contact to session store
   * @param gatewayId gateway ID
   * @param sessionId session ID
   * @return SessionState
   */
  def getSession(gatewayId: String, sessionId: String): SessionState

  /**
   * register SessionId to session store
   * @param gatewayId gateway ID
   * @param sessionId session ID
   * @param key registration key
   */
  def preregisterSession(gatewayId: String, sessionId: String, key: String): Unit

  /**
   * register Session to session store
   * @param gatewayId gateway ID
   * @param sessionId session ID
   * @param host Processor's host
   * @param port Processor's port
   * @param key registration key
   */
  def registerSession(gatewayId: String, sessionId: String, host: String, port: Int, key: String): Unit

  /**
   * delete Session
   * @param gatewayId gateway ID
   * @param sessionId session Id
   */
  def deleteSession(gatewayId: String, sessionId: String): Unit

  /**
   * gateway session lock
   * @param gatewayId gateway ID
   * @return Lock Object
   */
  def lock(gatewayId: String): SessionLock

  /**
   * gateway session unlock
   * @param lock Lock Object
   */
  def unlock(lock: SessionLock): Unit

  /**
   * register delete listener
   * @param gatewayId gateway Id
   * @param sessionId session Id
   * @param deleteFunction delete-event trigger, call function
   */
  def addDeleteListener(gatewayId: String, sessionId: String, deleteFunction: Function1[String, Unit] = null): Unit

  /**
   * initialize session store
   * @param gatewayId gateway Id
   */
  def registerGateway(gatewayId: String): Unit

  /**
   * close
   */
  def close(): Unit

  // --- utility method ---
  /**
   * extract Session Information for jsonString
   * @param jsonString
   * @return SessionInfo
   */
  def extractSessionInfo(jsonString: String): SessionState = {
    try {
      val jsonData = parse(jsonString)
      if (jsonData != JNothing && jsonData.children.length > 0) {
        val host = (jsonData \ "host") match {
          case JNothing => null
          case value: JValue => value.extract[String]
        }
        val port = (jsonData \ "port") match {
          case JNothing => null
          case value: JValue => value.extract[String]
        }
        val key = (jsonData \ "key") match {
          case JNothing => null
          case value: JValue => value.extract[String]
        }
        if (host == null && port == null && key == null) {
          SessionState.NotFound
        } else if (host == null && port == null && key != null) {
          SessionState.Registering(key)
        } else if (host != null && port != null && key != null) {
          SessionState.Ready(host, port.toInt, key)
        } else {
          SessionState.Inconsistent
        }
      } else {
        SessionState.NotFound
      }
    } catch {
      case e: Throwable =>
        throw e
    }
  }
}

/**
 * Session Lock Object
 */
class SessionLock(lock: Any) {
  val lockObject = lock
}

/**
 * SessionStore for non-persist
 */
class NonPersistentStore extends SessionStore {
  override def getAllSessions(gatewayId: String): Map[String, SessionState] = Map.empty[String, SessionState]
  override def getSession(gatewayId: String, sessionId: String): SessionState = SessionState.NotFound
  override def preregisterSession(gatewayId: String, sessionId: String, key: String): Unit = {}
  override def registerSession(gatewayId: String, sessionId: String, host: String, port: Int, key: String): Unit = {}
  override def deleteSession(gatewayId: String, sessionId: String): Unit = {}
  override def lock(gatewayId: String): SessionLock = { null }
  override def unlock(lock: SessionLock): Unit = {}
  override def addDeleteListener(gatewayId: String, sessionId: String, deleteFunction: Function1[String, Unit] = null): Unit = {}
  override def registerGateway(gatewayId: String): Unit = {}
  override def close(): Unit = {}
}

/**
 * SessionInfo
 */
trait SessionState

object SessionState {
  // Session ready
  case class Ready(host: String, port: Int, key: String) extends SessionState
  // Registering yet
  case class Registering(key: String) extends SessionState
  // Session Inconsistent
  case object Inconsistent extends SessionState
  // Unknown session
  case object NotFound extends SessionState
}
