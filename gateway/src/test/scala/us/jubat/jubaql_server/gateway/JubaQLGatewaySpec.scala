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

  "Parameter threads" should "return threads" in {
    // 省略時
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost"))
    result.get should not be None
    result.get.threads shouldBe (16)
    // オプション指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", "32"))
    result.get should not be None
    result.get.threads shouldBe (32)
    // オプション指定 最小値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", "1"))
    result.get should not be None
    result.get.threads shouldBe (1)
    // オプション指定 最大値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", s"${Int.MaxValue}"))
    result.get should not be None
    result.get.threads shouldBe (Int.MaxValue)
  }
  "Illegal parameter threads" should "out of range" in {
    // オプション指定 範囲外
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", "0"))
    result shouldBe (None)
    // オプション指定 範囲外
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", s"${Int.MaxValue + 1}"))
    result shouldBe (None)
    // オプション指定 指定なし
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--threads", ""))
    result shouldBe (None)
  }
  "Parameter channel_memory" should "return channelMemory" in {
    // 省略時
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost"))
    result.get should not be None
    result.get.channelMemory shouldBe (65536)
    // オプション指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", "256"))
    result.get should not be None
    result.get.channelMemory shouldBe (256)
    // オプション指定 最小値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", "0"))
    result.get should not be None
    result.get.channelMemory shouldBe (0)
    // オプション指定 最大値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", s"${Long.MaxValue}"))
    result.get should not be None
    result.get.channelMemory shouldBe (Long.MaxValue)
  }
  "Illegal parameter channel_memory" should "out of range" in {
    // オプション指定 範囲外
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", "-1"))
    result shouldBe (None)
    // オプション指定 範囲外
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", s"${Long.MaxValue + 1}"))
    result shouldBe (None)
    // オプション指定 指定なし
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--channel_memory", ""))
    result shouldBe (None)
  }
  "Parameter total_memory" should "return total_memory" in {
    // 省略時
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost"))
    result.get should not be None
    result.get.totalMemory shouldBe (1048576)
    // オプション指定
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", "256"))
    result.get should not be None
    result.get.totalMemory shouldBe (256)
    // オプション指定 最小値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", "0"))
    result.get should not be None
    result.get.totalMemory shouldBe (0)
    // オプション指定 最大値
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", s"${Long.MaxValue}"))
    result.get should not be None
    result.get.totalMemory shouldBe (Long.MaxValue)
  }
  "Illegal parameter total_memory" should "out of range" in {
    // オプション指定 範囲外
    var result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", "-1"))
    result shouldBe (None)
    // オプション指定 範囲外
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", s"${Long.MaxValue + 1}"))
    result shouldBe (None)
    // オプション指定 指定なし
    result = JubaQLGateway.parseCommandlineOption(Array("-i", "localhost", "--total_memory", ""))
    result shouldBe (None)
  }
}