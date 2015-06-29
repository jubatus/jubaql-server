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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.Filter
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

  it should "recognize CREATE DATASOURCE" taggedAs (LocalTest) in {
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

  it should "recognize CREATE DATASOURCE with keyword-named columns" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE DATASOURCE test1 (status string, count numeric, time boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
      """.stripMargin
    )

    result shouldNot be(None)
    val ds = result.get.asInstanceOf[CreateDatasource]
    ds.sourceName shouldBe "test1"
    ds.columns shouldBe List(("status", "string"), ("count", "numeric"), ("time", "boolean"))
    ds.sinkStorage shouldBe "hdfs://hello"
    ds.sinkStreams shouldBe List("fluentd://f1", "fluentd://f2")
  }

  it should "recognize CREATE DATASOURCE with non-ASCII column names" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE DATASOURCE test1 (ステータス string, count numeric, time boolean)
      FROM (STORAGE: "hdfs://hello", STREAM: "fluentd://f1", STREAM: "fluentd://f2")
      """.stripMargin
    )

    result shouldNot be(None)
    val ds = result.get.asInstanceOf[CreateDatasource]
    ds.sourceName shouldBe "test1"
    ds.columns shouldBe List(("ステータス", "string"), ("count", "numeric"), ("time", "boolean"))
    ds.sinkStorage shouldBe "hdfs://hello"
    ds.sinkStreams shouldBe List("fluentd://f1", "fluentd://f2")
  }

  // TODO write more CREATE MODEL tests

  it should "recognize CREATE MODEL" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH id CONFIG '{"test": 123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "id"))
    create.configJson shouldBe """{"test": 123}"""
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE MODEL with multi-line config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH id CONFIG '{"test":
      |123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "id"))
    create.configJson shouldBe "{\"test\":\n123}"
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE MODEL with non-ASCII characters" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: ラベル) AS 名前 WITH アイディー CONFIG '{"test": 123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "ラベル"))
    create.featureExtraction shouldBe List((NormalParameters("名前" :: Nil), "アイディー"))
    create.configJson shouldBe """{"test": 123}"""
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE MODEL with non-ASCII characters and wildcards" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: ラベル) AS 名前* WITH アイディー CONFIG '{"test": 123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "ラベル"))
    create.featureExtraction shouldBe List((WildcardWithPrefixParameter("名前"), "アイディー"))
    create.configJson shouldBe """{"test": 123}"""
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE STREAM FROM SELECT" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE STREAM teststream FROM SELECT * FROM test
      """.stripMargin
    )

    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromSelect]
    val create = result.get.asInstanceOf[CreateStreamFromSelect]
    create.streamName shouldBe "teststream"
  }

  it should "recognize CREATE STREAM FROM SELECT with non-ASCII characters" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE STREAM teststream FROM SELECT 名字 AS 名前 FROM test
      """.stripMargin
    )

    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromSelect]
  }

  it should "recognize CREATE TRIGGER" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE TRIGGER ON ds FOR EACH ROW EXECUTE someFunction(something)
      """.stripMargin
    )

    result shouldNot be(None)
    result.get shouldBe a[CreateTrigger]
    val create = result.get.asInstanceOf[CreateTrigger]
    create.dsName shouldBe "ds"
    create.condition shouldBe None
  }

  it should "recognize CREATE STREAM FROM SLIDING WINDOW" taggedAs(LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
    """
      |CREATE STREAM new_stream FROM
      |  SLIDING WINDOW (SIZE 4 ADVANCE 3 TUPLES)
      |  OVER source
      |  WITH fourier(some_col) AS fourier_coeffs
      |  WHERE id % 2 = 0
      |  HAVING fourier_coeffs = 2
    """.stripMargin)
    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromSlidingWindow]
    val create = result.get.asInstanceOf[CreateStreamFromSlidingWindow]
    create.streamName shouldBe "new_stream"
    create.windowSize shouldBe 4
    create.slideInterval shouldBe 3
    create.windowType shouldBe "tuples"
    create.source.children should not be empty
    create.source.children(0) shouldBe a[Filter]
    create.funcSpecs.size shouldBe 1
    create.funcSpecs(0)._1 shouldBe "fourier"
    create.funcSpecs(0)._2.size shouldBe 1
    create.funcSpecs(0)._3 shouldBe Some("fourier_coeffs")
    create.postCond shouldNot be(None)
  }

  it should "recognize CREATE STREAM FROM SLIDING WINDOW variations" taggedAs(LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
        |CREATE STREAM new_stream FROM
        |  SLIDING WINDOW (SIZE 4 ADVANCE 3 TIME)
        |  OVER source
        |  WITH histogram(7, some_col), avg(other_col) AS mean
      """.stripMargin)
    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromSlidingWindow]
    val create = result.get.asInstanceOf[CreateStreamFromSlidingWindow]
    create.streamName shouldBe "new_stream"
    create.windowSize shouldBe 4
    create.slideInterval shouldBe 3
    create.windowType shouldBe "time"
    create.source.children should not be empty
    create.source.children(0) shouldBe a[UnresolvedRelation]
    create.source.children(0).asInstanceOf[UnresolvedRelation]
      .tableIdentifier.head shouldBe "source"
    create.funcSpecs.size shouldBe 2
    create.funcSpecs(0)._1 shouldBe "histogram"
    create.funcSpecs(0)._2.size shouldBe 2
    create.funcSpecs(0)._3 shouldBe None
    create.funcSpecs(1)._1 shouldBe "avg"
    create.funcSpecs(1)._2.size shouldBe 1
    create.funcSpecs(1)._3 shouldBe Some("mean")
    create.postCond shouldBe None
  }

  it should "recognize CREATE STREAM FROM ANALYZE" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE STREAM emails_classified FROM
      |ANALYZE emails BY MODEL spam_filter USING classify AS spam
      """.stripMargin
    )

    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromAnalyze]
    val create = result.get.asInstanceOf[CreateStreamFromAnalyze]
    create.streamName shouldBe "emails_classified"
    create.analyze.data shouldBe "emails"
    create.newColumn shouldBe Some("spam")
  }

  it should "recognize CREATE STREAM FROM ANALYZE without column name" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
        |CREATE STREAM emails_classified FROM
        |ANALYZE emails BY MODEL spam_filter USING classify
      """.stripMargin
    )

    result shouldNot be(None)
    result.get shouldBe a[CreateStreamFromAnalyze]
    val create = result.get.asInstanceOf[CreateStreamFromAnalyze]
    create.streamName shouldBe "emails_classified"
    create.analyze.data shouldBe "emails"
    create.newColumn shouldBe None
  }

  it should "recognize LOG STREAM" taggedAs(LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOG STREAM source
      """.stripMargin
    )

    result shouldNot be(None)
    val log = result.get.asInstanceOf[LogStream]
    log.streamName shouldBe "source"
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
    analyze.data shouldBe """{"test": 123}"""
  }

  it should "recognize STATUS" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("STATUS")
    result should not be empty
    result.get shouldBe a[Status]
  }

  it should "recognize SHUTDOWN" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("SHUTDOWN")
    result should not be empty
    result.get shouldBe a[Shutdown]
  }

  it should "recognize START PROCESSING" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("START PROCESSING someDs")
    result should not be empty
    result.get shouldBe a[StartProcessing]
    result.get.asInstanceOf[StartProcessing].dsName shouldBe("someDs")
  }

  it should "recognize STOP PROCESSING" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse("STOP PROCESSING")
    result should not be empty
    result.get shouldBe a[StopProcessing]
  }

  it should "recognize CREATE FUNCTION" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
    """
      |CREATE FUNCTION func(arg string) RETURNS string
      |LANGUAGE JavaScript AS $$ var n = 1; return n; $$
    """.stripMargin
    )
    result should not be empty
    result.get shouldBe a[CreateFunction]
    val cf = result.get.asInstanceOf[CreateFunction]
    cf.funcName shouldBe "func"
    cf.args shouldBe List(("arg", "string"))
    cf.returnType shouldBe "string"
    cf.lang shouldBe "JavaScript"
  }

  val jsSnippets =
    ("one simple line", "return x;") ::
      ("line breaks", "var a = 1;\nreturn x;") ::
      ("dollar characters", """$.sendMail("me@you.de");""") ::
      ("multiple dollar characters", """$.sendMail("me@you.de", "money $!");""") ::
      Nil

  jsSnippets.foreach(kv => {
    val (desc, js) = kv
    it should s"recognize CREATE FUNCTION code with $desc" taggedAs (LocalTest) in {
      val parser = new JubaQLParser
      val stmtTmpl = """
                       |CREATE FUNCTION func(arg string) RETURNS string
                       |LANGUAGE JavaScript AS $$
                       |%s
                       |$$
                     """.stripMargin
      val stmt = stmtTmpl.format(js)
      val result = parser.parse(stmt)
      result should not be empty
      result.get.asInstanceOf[CreateFunction].body.trim shouldBe js
    }
  })

  it should "recognize CREATE FEATURE FUNCTION" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
        |CREATE FEATURE FUNCTION func(arg string)
        |LANGUAGE JavaScript AS $$ var n = 1; return n; $$
      """.stripMargin
    )
    result should not be empty
    result.get shouldBe a[CreateFeatureFunction]
    val cf = result.get.asInstanceOf[CreateFeatureFunction]
    cf.funcName shouldBe "func"
    cf.args shouldBe List(("arg", "string"))
    cf.lang shouldBe "JavaScript"
    cf.body shouldBe " var n = 1; return n; "
  }

  it should "recognize CREATE TRIGGER FUNCTION" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
        |CREATE TRIGGER FUNCTION func(arg string)
        |LANGUAGE JavaScript AS $$ var n = 1; return n; $$
      """.stripMargin
    )
    result should not be empty
    result.get shouldBe a[CreateTriggerFunction]
    val cf = result.get.asInstanceOf[CreateTriggerFunction]
    cf.funcName shouldBe "func"
    cf.args shouldBe List(("arg", "string"))
    cf.lang shouldBe "JavaScript"
    cf.body shouldBe " var n = 1; return n; "
  }
}
