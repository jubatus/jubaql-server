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

import java.net.URLDecoder

import org.scalatest.{FlatSpec, ShouldMatchers}
import org.subethamail.wiser.Wiser
import unfiltered.request._
import unfiltered.response._
import unfiltered.util.RunnableServer

import scala.collection.JavaConversions._
import scala.util.Success
import org.scalatest.exceptions.TestFailedException
import javax.script.ScriptException
import scala.util.Failure

class JavaScriptSpec extends FlatSpec with ShouldMatchers with MockServer {
  protected var wiser: Wiser = null
  val funcBodyTmpl = s"function test(x) { %s }"

  "JavaScript" should "allow simple functions" taggedAs (LocalTest) in {
    val body = "return x;"
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[Double]("test",
      1, funcBody, Double.box(17.0))
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe 17.0
    })
  }

  it should "allow accessing Scala object functions" taggedAs (LocalTest) in {
    val body =
      """
        |return jql.test();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe "test"
    })
  }

  it should "allow GETting from an HTTP server" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/test";
        |var result = jql.httpGet(url);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe "thanks for your GET"
    })
  }

  it should "allow GETting from an HTTP server with key-value list" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/test";
        |var obj = {"user": 1234, "msg": "こんにちは"};
        |var result = jql.httpGet(url, obj);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe "thanks for your GET with msg 'こんにちは'"
    })
  }

  it should "allow (some) parallel HTTP requests" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/sleep";
        |var result = jql.httpGet(url);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val cores = Runtime.getRuntime().availableProcessors();
    // up to 8 requests are processed in parallel, the 9th is
    // executed later (seems like 8 is the thread pool limit for
    // either dispatch or unfiltered)
    // modify: get number of cores
    val loop = (1 to cores).toList.par
    val startTime = System.currentTimeMillis()
    val resultOpts = loop.map(_ => {
      JavaScriptUDFManager.registerAndTryCall[String]("test", 0, funcBody)
    }).seq
    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime
    info("%s parallel HTTP calls took %s ms".format(loop.size, duration))
    duration should be < 2000L
    resultOpts.foreach(resultOpt => {
      resultOpt shouldBe a[Success[_]]
      resultOpt.foreach(result => {
        result shouldBe "I slept a bit"
      })
    })
  }

  it should "allow POSTing to an HTTP server" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/test";
        |var result = jql.httpPost(url);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe "thanks for your POST"
    })
  }

  it should "allow POSTing a JSON-ified Object to an HTTP server" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/test";
        |var obj = {"user": 1234, "msg": "こんにちは"};
        |var json = JSON.stringify(obj);
        |var result = jql.httpPost(url, json);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result should startWith("thanks for your POST with body:")
      result should include("こんにちは")
    })
  }

  it should "allow POSTing a key-value list to an HTTP server" taggedAs (LocalTest) in {
    val body =
      """
        |var url = "http://localhost:12345/test";
        |var obj = {"user": 1234, "msg": "こんにちは"};
        |var result = jql.httpPost(url, obj);
        |if (result.isFailure())
        |  return result.failed().get().getMessage();
        |else
        |  return result.get();
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    resultOpt.foreach(result => {
      result shouldBe "thanks for your POST with msg 'こんにちは'"
    })
  }

  it should "allow sending emails" taggedAs (LocalTest) in {
    val body =
      """
        |jql.sendMail("localhost", 1025,
        |  "me@privacy.net", "root@localhost",
        |  "こんにちは",
        |  "Just testing email. よろしく。");
      """.stripMargin
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndTryCall[String]("test",
      0, funcBody)
    resultOpt shouldBe a[Success[_]]
    wiser.getMessages should not be empty
    val msg = wiser.getMessages.head
    val mime = msg.getMimeMessage
    mime.getFrom.head.toString shouldBe "me@privacy.net"
    mime.getAllRecipients.head.toString shouldBe "root@localhost"
    mime.getSubject shouldBe "こんにちは"
    mime.getContent.toString should include("よろしく")
  }

  it should "registerAndCall: allow simple functions" taggedAs (LocalTest) in {
    val body = "return x;"
    val funcBody = funcBodyTmpl.format(body)
    val result = JavaScriptUDFManager.registerAndCall[Double]("test",
      1, funcBody, Double.box(17.0))
    result shouldBe 17.0
  }

  it should "registerAndTryCall: allow simple functions" taggedAs (LocalTest) in {
    val body = "return x;"
    val funcBody = funcBodyTmpl.format(body)
    val resultTry = JavaScriptUDFManager.registerAndTryCall[Double]("test",
      1, funcBody, Double.box(17.0))
    resultTry shouldBe a[Success[_]]
    resultTry.foreach(result => {
      result shouldBe 17.0
    })
  }

  it should "registerAndOptionCall: allow simple functions" taggedAs (LocalTest) in {
    val body = "return x;"
    val funcBody = funcBodyTmpl.format(body)
    val resultOpt = JavaScriptUDFManager.registerAndOptionCall[Double]("test",
      1, funcBody, Double.box(17.0))
    resultOpt shouldBe a[Some[_]]
    resultOpt.foreach(result => {
      result shouldBe 17.0
    })
  }

  "JavaScript throws Exception" should "registerAndCall(args = 1) throw Exception" taggedAs (LocalTest) in {
    val body = "throw new Error('error Message');"
    val funcBody = funcBodyTmpl.format(body)
    try {
      val result = JavaScriptUDFManager.registerAndCall[Double]("test",
        1, funcBody, Double.box(17.0))
      fail()
    } catch {
      case e: TestFailedException =>
        e.printStackTrace()
        fail()
      case e: Exception =>
        // invoke methodの出力メッセージ確認
        e.getMessage should startWith("Failed to invoke function. functionName: test, args: WrappedArray(17.0)")
    }
  }

  it should "registerAndCall(args = 0) throw Exception" taggedAs (LocalTest) in {
    val body = "throw new Error('error Message');"
    val funcBody = funcBodyTmpl.format(body)
    try {
      val result = JavaScriptUDFManager.registerAndCall[Double]("test",
        0, funcBody)
      fail()
    } catch {
      case e: TestFailedException =>
        e.printStackTrace()
        fail()
      case e: Exception =>
        // invoke methodの出力メッセージ確認(パラメータなし)
        e.getMessage should startWith("Failed to invoke function. functionName: test, args: WrappedArray()")
    }
  }

  it should "registerAndTryCall return Failure" taggedAs (LocalTest) in {
    val body = "throw new Error('error Message');"
    val funcBody = funcBodyTmpl.format(body)
    val resultTry = JavaScriptUDFManager.registerAndTryCall[Double]("test",
      1, funcBody, Double.box(17.0))
    resultTry shouldBe a[Failure[_]]
    resultTry.foreach(result => {
      result shouldBe "Failed to invoke function. functionName: test, args: WrappedArray(17.0)"
    })
  }

  it should "registerAndOptionCall return None" taggedAs (LocalTest) in {
    val body = "throw new Error('error Message');"
    val funcBody = funcBodyTmpl.format(body)
    val resultTry = JavaScriptUDFManager.registerAndOptionCall[Double]("test",
      1, funcBody, Double.box(17.0))
    resultTry shouldBe None
  }

  // this server mocks the gateway
  protected val server: RunnableServer = {
    unfiltered.netty.Server.http(12345).plan(
      // define the server behavior
      unfiltered.netty.cycle.Planify {
        // GET with no parameters
        case req@GET(Path(Seg("test" :: Nil))) if req.parameterNames.isEmpty =>
          Ok ~> ResponseString("thanks for your GET")
        // GET with URL parameters
        case req@GET(Path(Seg("test" :: Nil))) =>
          val message = req.parameterValues("msg").head
          Ok ~> ContentType("text/plain; charset=utf-8") ~>
            ResponseString(s"thanks for your GET with msg '$message'")
        // simulate long computation
        case req@GET(Path(Seg("sleep" :: Nil))) =>
          Thread.sleep(1000)
          Ok ~> ResponseString("I slept a bit")
        // POST
        case req@POST(Path(Seg("test" :: Nil))) =>
          val body = Body.string(req)
          // POST with no body
          if (body.isEmpty) {
            Ok ~> ResponseString("thanks for your POST")
          }
          // POST with form-encoded values
          else if (req.headers("Content-Type").exists(_.contains("form-urlencoded"))) {
            val parts = body.split("&").map(s => {
              (s.split("=")(0), s.split("=")(1))
            }).toMap
            val message = URLDecoder.decode(parts("msg"), "utf-8")
            Ok ~> ContentType("text/plain; charset=utf-8") ~>
              ResponseString(s"thanks for your POST with msg '$message'")
          }
          // POST with other body
          else {
            Ok ~> ContentType("text/plain; charset=utf-8") ~>
              ResponseString("thanks for your POST with body: " + body)
          }
        case _ =>
          NotFound ~> ResponseString("404")
      })
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    wiser = new Wiser()
    wiser.setPort(1025)
    wiser.start()
  }

  override protected def afterAll(): Unit = {
    wiser.stop()
    super.afterAll()
  }
}
