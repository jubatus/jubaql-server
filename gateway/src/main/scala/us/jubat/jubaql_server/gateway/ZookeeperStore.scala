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

import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.mutable
import scala.collection.mutable._
import org.apache.curator._
import org.apache.curator.retry._
import org.apache.curator.framework._
import org.apache.curator.framework.recipes.locks._
import collection.JavaConversions._
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.curator.framework.api.CuratorListener
import org.apache.curator.framework.api.CuratorEvent
import org.apache.zookeeper.Watcher
import org.apache.curator.framework.listen.ListenerContainer
import java.util.concurrent.TimeUnit

/**
 * ZooKeeper Store Class
 */
class ZookeeperStore extends SessionStore with LazyLogging {

  val zkJubaQLPath: String = "/jubaql"
  val zkSessionPath: String = "/jubaql/session"

  val lockNodeName = "locks"
  val leasesNodeName = "leases"
  //lock keep time(unit: seconds)
  val lockTimeout: Long = 300
  // retry sleep time(unit: ms)
  val retrySleepTimeMs: Int = 1000
  val retryCount: Int = 1

  val zookeeperString: String = scala.util.Properties.propOrElse("jubaql.zookeeper", "")
  val retryPolicy: RetryPolicy = new RetryNTimes(retryCount, retrySleepTimeMs)

  logger.info(s"connecting to ZooKeeper : ${zookeeperString}")
  val zookeeperClient: CuratorFramework = CuratorFrameworkFactory.newClient(zookeeperString, retryPolicy)

  zookeeperClient.start()
  if (!zookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut) {
    zookeeperClient.close()
    logger.error(s"zookeeper connection timeout. zookeeper: ${zookeeperString}")
    throw new Exception("failed to connected zookeeper")
  }
  logger.info(s"connected to ZooKeeper : ${zookeeperString}")

  override def getAllSessions(gatewayId: String): Map[String, SessionState] = {
    var result = Map.empty[String, SessionState]
    try {
      val isExist = zookeeperClient.checkExists().forPath(s"${zkSessionPath}/${gatewayId}")
      if (isExist != null) {
        val sessionIdList = zookeeperClient.getChildren.forPath(s"${zkSessionPath}/${gatewayId}")
        for (sessionId <- sessionIdList) {
          val sessionInfo = getSession(gatewayId, sessionId)
          result += sessionId -> sessionInfo
        }
      } else {
        logger.debug(s"non exist node. gatewayId: ${gatewayId}")
      }
    } catch {
      case e: Exception =>
        val errorMessage = "failed to get all session."
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
    return result
  }

  override def getSession(gatewayId: String, sessionId: String): SessionState = {
    try {
      val sessionByteArray = zookeeperClient.getData().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}")
      val sessionJsonString = new String(sessionByteArray, "UTF-8")
      extractSessionInfo(sessionJsonString)
    } catch {
      case e: NoNodeException =>
        logger.debug(s"not found session. sesionId: ${sessionId}")
        SessionState.NotFound
      case e: Exception =>
        val errorMessage = s"failed to get session. sessionId: ${sessionId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def preregisterSession(gatewayId: String, sessionId: String, key: String): Unit = {
    try {
      val isExists = zookeeperClient.checkExists().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}")
      if (isExists == null) {
        zookeeperClient.create().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}", s"""{"key":"$key"}""".getBytes("UTF-8"))
      } else {
        throw new IllegalStateException(s"already exists session. sessionId: ${sessionId}")
      }
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to pre-register session. sessionId: ${sessionId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def registerSession(gatewayId: String, sessionId: String, host: String, port: Int, key: String): Unit = {
    try {
      val isExists = zookeeperClient.checkExists().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}")
      if (isExists != null) {
        val currentSession = getSession(gatewayId, sessionId)
        if (currentSession.isInstanceOf[SessionState.Registering]) {
          zookeeperClient.setData().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}", s"""{"host":"$host","port":$port,"key":"$key"}""".getBytes("UTF-8"))
        } else {
          throw new IllegalStateException(s"illegal session state. sessionId: ${sessionId}, state: ${currentSession}")
        }
      } else {
        throw new IllegalStateException(s"non exists session. sessionId: ${sessionId}")
      }
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to register session. sessionId: ${sessionId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def deleteSession(gatewayId: String, sessionId: String): Unit = {
    try {
      zookeeperClient.delete().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}")
    } catch {
      case e: NoNodeException => logger.warn(s"No exist Node. sessionId : $sessionId")
      case e: Exception =>
        val errorMessage = s"failed to delete session. sessionId: ${sessionId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def lock(gatewayId: String): SessionLock = {
    val mutex: InterProcessSemaphoreMutex = try {
      val mutex = new InterProcessSemaphoreMutex(zookeeperClient, s"${zkSessionPath}/${gatewayId}")
      mutex.acquire(lockTimeout, TimeUnit.SECONDS)
      mutex
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to create lock object. gatewayId: ${gatewayId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
    new SessionLock(mutex)
  }

  override def unlock(lock: SessionLock): Unit = {
    if (lock != null) {
      val mutex = lock.lockObject
      if (mutex != null && mutex.isInstanceOf[InterProcessSemaphoreMutex]) {
        try {
          mutex.asInstanceOf[InterProcessSemaphoreMutex].release()
        } catch {
          case e: Exception =>
            val errorMessage = "failed to unlock"
            logger.error(errorMessage, e)
            throw new Exception(errorMessage, e)
        }
      } else {
        val errorMessage = if (mutex != null) {
          s"failed to unlock. illegal lock object: ${mutex.getClass()}"
        } else {
          "failed to unlock. lock object: null"
        }
        logger.error(errorMessage)
        throw new Exception(errorMessage)
      }
    } else {
      val errorMessage = "failed to unlock. session lock: null"
      logger.error(errorMessage)
      throw new Exception(errorMessage)
    }
  }

  override def addDeleteListener(gatewayId: String, sessionId: String, deleteFunction: Function1[String, Unit]): Unit = {
    try {
      zookeeperClient.synchronized {
        val listenerSize = if (zookeeperClient.getCuratorListenable.isInstanceOf[ListenerContainer[CuratorListener]]) {
          val container = zookeeperClient.getCuratorListenable.asInstanceOf[ListenerContainer[CuratorListener]]
          container.size()
        } else {
          throw new Exception(s"invalid listener class. class: ${zookeeperClient.getCuratorListenable.getClass}")
        }
        if (sessionId != lockNodeName && sessionId != leasesNodeName) {
          if (listenerSize == 0) {
            val listener = new CuratorListener() {
              override def eventReceived(client: CuratorFramework, event: CuratorEvent): Unit = {
                if (event.getWatchedEvent != null && event.getWatchedEvent.getType == Watcher.Event.EventType.NodeDeleted) {
                  val deletedNodePath = event.getPath
                  val deletedSessionID = deletedNodePath.substring(deletedNodePath.lastIndexOf("/") + 1, deletedNodePath.length)
                  deleteFunction(deletedSessionID)
                }
              }
            }
            zookeeperClient.getCuratorListenable.addListener(listener)
          }
          zookeeperClient.getChildren.watched().forPath(s"${zkSessionPath}/${gatewayId}/${sessionId}")
        } else {
          logger.debug(s"not add listener. exclude sessionId: ${sessionId}")
        }
      }
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to add delete listener. sessionId: ${sessionId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def registerGateway(gatewayId: String): Unit = {
    try {
      if (zookeeperClient.checkExists().forPath(zkJubaQLPath) == null) {
        zookeeperClient.create().forPath(zkJubaQLPath, new Array[Byte](0))
      }
      if (zookeeperClient.checkExists().forPath(zkSessionPath) == null) {
        zookeeperClient.create().forPath(zkSessionPath, new Array[Byte](0))
      }
      if (zookeeperClient.checkExists().forPath(s"${zkSessionPath}/${gatewayId}") == null) {
        zookeeperClient.create().forPath(s"${zkSessionPath}/${gatewayId}", new Array[Byte](0))
      }
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to registerGateway. gatewayId: ${gatewayId}"
        logger.error(errorMessage, e)
        throw new Exception(errorMessage, e)
    }
  }

  override def close(): Unit = {
    try {
      zookeeperClient.close()
      logger.info("zookeeperClient closed")
    } catch {
      case e: Exception =>
        val errorMessage = s"failed to close."
        logger.error(errorMessage, e)
    }
  }
}