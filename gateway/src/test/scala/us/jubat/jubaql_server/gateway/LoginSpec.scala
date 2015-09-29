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

class LoginSpec extends FlatSpec with Matchers with GatewayServer {

  implicit val formats = DefaultFormats

  val url = :/("localhost", 9877) / "login"
  val pro_url = :/("localhost", 9878) / "login"
  val dev_url = :/("localhost", 9879) / "login"


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

  "POST to /login(Production Mode)" should "return a JSON which contains session_id" in {
    val req = Http(pro_url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
    val maybeSessionId = maybeJson.get.extractOpt[SessionId]
    maybeSessionId should not be None
    //セッションIDの返却チェック
    maybeSessionId.get.session_id.length should be > 0
    //TODO JubaQL Processorのアプリケーション名とJubaQL Processorへの引数をテストの実行ログで確認(目視) -> 自動チェック化が必要
  }

  "POST to /login(Development Mode)" should "return a JSON which contains session_id" in {
    val req = Http(dev_url.POST OK as.String)
    req.option.apply() should not be None
    val returnedString = req.option.apply.get
    val maybeJson = parseOpt(returnedString)
    maybeJson should not be None
    val maybeSessionId = maybeJson.get.extractOpt[SessionId]
    maybeSessionId should not be None
    //セッションIDの返却チェック
    maybeSessionId.get.session_id.length should be > 0
    //TODO JubaQL Processorのアプリケーション名をテストの実行ログで確認(目視) -> 自動チェック化が必要
  }
}
