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
package us.jubat.jubaql_server.processor

import scala.collection.mutable
import scala.collection.JavaConversions
import javax.script.{ScriptEngine, ScriptEngineManager, Invocable}

import scala.util.{Failure, Success, Try}

class JavaScriptUDFManager {
  // The null is required.
  // See: http://stackoverflow.com/questions/20168226/sbt-0-13-scriptengine-is-null-for-getenginebyname-javascript
  private val scriptEngineManager = new ScriptEngineManager(null)

  private val jsEngines = new ThreadLocal[ScriptEngine] {
    override def initialValue() = createScriptEngine()
  }

  private case class Mapped(nargs: Int, funcBody: String, var threadIds: List[Long])
  private val funcs = new mutable.HashMap[String, Mapped]

  // throws javax.script.ScriptException when funcBody is invalid.
  def register(funcName: String, nargs: Int, funcBody: String): Unit = {
    val engine = getScriptEngine()
    val threadId = Thread.currentThread.getId

    funcs.synchronized {
      def overwriteFunc(): Unit = {
        funcs += (funcName -> Mapped(nargs, funcBody, List(threadId)))
      }

      funcs.get(funcName) match {
        case None =>
          overwriteFunc()
        case Some(m) if funcBody != m.funcBody =>
          overwriteFunc()

        case Some(m) =>
          if (m.threadIds.contains(threadId))
            return
          m.threadIds = threadId :: m.threadIds
      }
      engine.eval(funcBody)
    }
  }

  private def invoke(funcName: String, args: AnyRef*): AnyRef = {
    val inv = getInvocableEngine()
    inv.invokeFunction(funcName, args: _*)
  }

  def call[T](funcName: String, args: AnyRef*): Option[T] = {
    Try {
      invoke(funcName, args:_*).asInstanceOf[T]
    } match {
      case Success(value) => Some(value)
      case Failure(err) => None
    }
  }

  def tryCall[T](funcName: String, args: AnyRef*): Try[T] = Try {
    invoke(funcName, args:_*).asInstanceOf[T]
  }

  def registerAndCall[T](funcName: String, nargs: Int, funcBody: String, args: AnyRef*): Option[T] = {
    register(funcName, nargs, funcBody)
    call[T](funcName, args:_*)
  }

  def registerAndTryCall[T](funcName: String, nargs: Int, funcBody: String, args: AnyRef*): Try[T] = {
    register(funcName, nargs, funcBody)
    tryCall[T](funcName, args:_*)
  }

  def getNumberOfArgsByFunctionName(fname: String): Option[Int] = funcs.synchronized {
    funcs.get(fname).map(_.nargs)
  }

  // This method is required because Rhino may return ConsString (!= java.lang.String)
  def asScala(x: AnyRef) = {
    val inv = getInvocableEngine
    inv.invokeMethod(JavaScriptHelpers, "javaScriptToScala", x)
  }

  private def getScriptEngine(): ScriptEngine = jsEngines.get

  private def getInvocableEngine(): Invocable = {
    getScriptEngine().asInstanceOf[Invocable]
  }

  private def createScriptEngine(): ScriptEngine = {
    var engine: ScriptEngine = null
    scriptEngineManager.synchronized {
      engine = scriptEngineManager.getEngineByName("JavaScript")
    }
    if (engine == null) {
      val threadId = Thread.currentThread.getId
      throw new Exception("failed to create JavaScript engine in thread %d".format(threadId))
    }
    engine.put("jql", JavaScriptHelpers)

    engine
  }
}

object JavaScriptUDFManager extends JavaScriptUDFManager

object JavaScriptFeatureFunctionManager extends JavaScriptUDFManager {
  def callAndGetValues(funcName: String, args: AnyRef*): Map[String, Any] = {
    tryCall[java.util.Map[String, AnyRef]](funcName, args:_*) match {
      case Success(m) =>
        JavaConversions.mapAsScalaMap(m).toMap.mapValues(asScala)
      case Failure(err) =>
        throw err
    }
  }
}
