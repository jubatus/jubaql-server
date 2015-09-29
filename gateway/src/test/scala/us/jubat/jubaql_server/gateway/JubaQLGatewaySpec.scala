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

class JubaQLGatewaySpec extends FlatSpec with Matchers {

  "Parameter gatewyId" should "return gatewayid" in {
    // 省略時。パラメータから取得するgatewayIDは空。後で[host_port]の形式で生成する。
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost"))
    result.get should not be None
    result.get.gatewayId shouldBe ("")
    // 1文字オプションによるgatewayID指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "-g", "gatewayid"))
    result.get should not be None
    result.get.gatewayId shouldBe ("gatewayid")
    // ロングオプションによるgatewayID指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--gatewayID", "gatewayid2"))
    result.get should not be None
    result.get.gatewayId shouldBe ("gatewayid2")
  }
  "Parameter persist" should "return persist" in {
    // 省略時
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost"))
    result.get should not be None
    result.get.persist shouldBe (false)
    // オプション指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--persist"))
    result.get should not be None
    result.get.persist shouldBe (true)
  }
  "Illegal parameter" should "stderr usage" in {
    // 不正オプション時のusgageチェック
    val result = JubaQLGateway.parseCommandlineOption(Array("-i"))
    result shouldBe (None)
    // Usageは目視確認
  }

}