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

import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, FlatSpec}
import org.apache.spark.SparkContext

/* This test case tests only the state-independent (helper) functions of
 * JubaQLService (such as `parseJson()` or `extractDatum()`). It does
 * not test interaction with external components or anything that
 * requires state change.
 * (The reason being that we need to kill the JVM that is running
 * the JubaQLProcessor to exit cleanly.)
 */
class JubaQLServiceHelperSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
  private var sc: SparkContext = null
  private var service: JubaQLServiceTester = null

  // create a subclass to test the protected methods
  class JubaQLServiceTester(sc: SparkContext) extends JubaQLService(sc, RunMode.Development) {
    override def parseJson(in: String): Option[JubaQLAST] =
      super.parseJson(in)
  }

  "parseJson()" should "be able to parse JSON" taggedAs (LocalTest) in {
    val query = """
      CREATE DATASOURCE test1 (column_type1 string, column_type2 numeric, column_type3 boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
                """.stripMargin.trim
    val json = """{"query": "%s"}""".format(query.replace("\"", "\\\""))
    val result = service.parseJson(json)
    result should not be empty
    result.get shouldBe a[CreateDatasource]
  }

  it should "be able to parse JSON with additional fields" taggedAs (LocalTest) in {
    val query = """
      CREATE DATASOURCE test1 (column_type1 string, column_type2 numeric, column_type3 boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
                """.stripMargin.trim
    val json = """{"session_id": "test", "query": "%s"}""".format(query.replace("\"", "\\\""))
    val result = service.parseJson(json)
    result should not be empty
    result.get shouldBe a[CreateDatasource]
  }

  it should "yield None if the JSON contains a bogus query" taggedAs (LocalTest) in {
    val json = """{"query": "test"}"""
    val result = service.parseJson(json)
    result shouldBe empty
  }

  it should "yield None if the JSON contains a non-string query" taggedAs (LocalTest) in {
    val json = """{"query": 27}"""
    val result = service.parseJson(json)
    result shouldBe empty
  }

  it should "yield None if the JSON contains no query" taggedAs (LocalTest) in {
    val json = """{"foo": "bar"}"""
    val result = service.parseJson(json)
    result shouldBe empty
  }

  it should "yield None if the string is no JSON" taggedAs (LocalTest) in {
    val json = """{"foo": "bar"}"""
    val result = service.parseJson(json)
    result shouldBe empty
  }

  override protected def beforeAll(): Unit = {
    sc = new SparkContext("local[3]", "JubaQL Processor Test")
    service = new JubaQLServiceTester(sc)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
