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

import us.jubat.jubaql_server.gateway.json.Query
import org.scalatest._
import EitherValues._
import dispatch._
import dispatch.Defaults._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

// We use mock processor in this test, so queries are dummies.

class JubaQLSpec extends FlatSpec with Matchers with ProcessorAndGatewayServer {
  val jubaQlUrl  = :/("localhost", 9877) / "jubaql"

  implicit val formats = DefaultFormats

  def requestAsJson() = {
    val request = (jubaQlUrl).POST
    request.setContentType("application/json", "UTF-8")
  }

  "Posting jubaql with not a JSON" should "fail" in {
    val request = requestAsJson() << "abc"
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with an empty JSON object" should "fail" in {
    val request = requestAsJson() << "{}"
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql without session_id" should "fail" in {
    val request = requestAsJson() << """{"query": "query"}"""
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql without query" should "fail" in {
    val request = requestAsJson() << f"""{"session_id": "$session%s"}"""
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 400
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with unknown session_id" should "fail" in {
    val request = requestAsJson() << write(Query("NOSUCHID", "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 401
    result.right.value.getContentType should include("charset=utf-8")
  }

  "Posting jubaql with a valid JSON" should "succeed" in {
    val request = requestAsJson() << write(Query(session, "query")).toString
    val result = Http(request > (x => x)).either.apply()
    result.right.value.getStatusCode shouldBe 200
    result.right.value.getContentType should include("charset=utf-8")
  }
}
