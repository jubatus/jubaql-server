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

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

/* This test case tests only that the parser recognizes the syntax
 * that was defined for JubaQL correctly. It does not test the
 * functionality that was defined for each statement.
 */
class JubaQLParserSpec extends FlatSpec {
  // TODO write more CREATE DATASOURCE tests

  "A JubaQLParser" should "recognize CREATE DATASOURCE without schema" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE DATASOURCE test1
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
      """.stripMargin
    )

    result shouldNot be(None)
    val ds = result.get.asInstanceOf[CreateDatasource]
    ds.sourceName shouldBe "test1"
    ds.columns shouldBe List()
    ds.sinkStorage shouldBe "hdfs://hello"
    ds.sinkStreams shouldBe List("fluentd://f1", "fluentd://f2")
  }

  "A JubaQLParser" should "recognize CREATE DATASOURCE" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE DATASOURCE test1 (column_type1 string, column_type2 numeric, column_type3 boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
      """.stripMargin
    )

    result shouldNot be(None)
    val ds = result.get.asInstanceOf[CreateDatasource]
    ds.sourceName shouldBe "test1"
    ds.columns shouldBe List(("column_type1", "string"), ("column_type2", "numeric"), ("column_type3", "boolean"))
    ds.sinkStorage shouldBe "hdfs://hello"
    ds.sinkStreams shouldBe List("fluentd://f1", "fluentd://f2")
  }

  // TODO write more CREATE MODEL tests

  it should "recognize CREATE MODEL" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE ANOMALY MODEL test1 WITH(id: "id", datum: ["a", "b"]) config = '{"test": 123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "ANOMALY"
    create.modelName shouldBe "test1"
    create.configJson shouldBe "{\"test\": 123}"
    create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  // TODO write more UPDATE tests

  it should "recognize UPDATE" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train FROM source
      """.stripMargin
    )

    result shouldNot be(None)
    val update = result.get.asInstanceOf[Update]
    update.modelName shouldBe "juba_model"
    update.rpcName shouldBe "train"
    update.source shouldBe "source"
  }

  // TODO write more ANALYZE tests

  it should "recognize ANALYZE" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      ANALYZE '{"test": 123}' BY MODEL juba_model USING calc_score
      """.stripMargin
    )

    result shouldNot be(None)
    val analyze = result.get.asInstanceOf[Analyze]
    analyze.modelName shouldBe "juba_model"
    analyze.rpcName shouldBe "calc_score"
    analyze.data shouldBe "{\"test\": 123}"
  }

  it should "recognize SHUTDOWN" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("SHUTDOWN")
    result should not be empty
    result.get shouldBe a[Shutdown]
  }

  it should "recognize STOP PROCESSING" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("STOP PROCESSING")
    result should not be empty
    result.get shouldBe a[StopProcessing]
  }
}
