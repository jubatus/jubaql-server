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

import java.net.URI

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
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.jubatusConfigJsonOrPath.isLeft shouldBe true
    create.jubatusConfigJsonOrPath.left.get shouldBe """{"test": 123}"""
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE MODEL with multi-line config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test":
      |123}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.jubatusConfigJsonOrPath.isLeft shouldBe true
    create.jubatusConfigJsonOrPath.left.get shouldBe "{\"test\":\n123}"
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
    create.jubatusConfigJsonOrPath.isLeft shouldBe true
    create.jubatusConfigJsonOrPath.left.get shouldBe """{"test": 123}"""
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
    create.jubatusConfigJsonOrPath.isLeft shouldBe true
    create.jubatusConfigJsonOrPath.left.get shouldBe """{"test": 123}"""
    //create.specifier shouldBe List(("id", List("id")), ("datum", List("a", "b")))
  }

  it should "recognize CREATE MODEL with config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG FILE 'file:///tmp/config/jubatusConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.jubatusConfigJsonOrPath.isRight shouldBe true
    create.jubatusConfigJsonOrPath.right.get shouldBe new URI("file:///tmp/config/jubatusConfig.json")
  }

  it should "recognize CREATE MODEL with illegal config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    try{
      val result: Option[JubaQLAST] = parser.parse(
        """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG FILE '{"test": 123}'
      """.stripMargin)
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.URISyntaxException])
    }
  }

  it should "recognize CREATE MODEL with CONFIG FILES(wrong query)" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG FILES 'file:///tmp/config/jubatusConfig.json'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL without recource config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.resConfigJsonOrPath shouldBe None
  }

  it should "recognize CREATE MODEL for recource config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' RESOURCE CONFIG '{"applicationmaster_memory": 256}'
      """.stripMargin)
    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.resConfigJsonOrPath shouldBe Some(Left("""{"applicationmaster_memory": 256}"""))
  }

  it should "not recognize CREATE MODEL for recource config without value" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' RESOURCE CONFIG
      """.stripMargin)
    result shouldBe (None)
  }

  it should "not recognize CREATE MODEL for recource config without 'CONFIG'" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' RESOURCE '{"applicationmaster_memory": 256}'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL with resource config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' RESOURCE CONFIG FILE 'file:///tmp/config/resourceConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.resConfigJsonOrPath shouldNot be(None)
    create.resConfigJsonOrPath.get.isRight shouldBe true
    create.resConfigJsonOrPath.get.right.get shouldBe new URI("file:///tmp/config/resourceConfig.json")
  }

  it should "recognize CREATE MODEL with illegal resource config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    try{
      val result: Option[JubaQLAST] = parser.parse(
        """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG FILE '{"test": 123}' RESOURCE CONFIG FILE '{"test": 123}'
      """.stripMargin)
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.URISyntaxException])
    }
  }

  it should "recognize CREATE MODEL with RESOURCE CONFIG FILES(wrong query)" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' RESOURCE CONFIG FILES 'file:///tmp/config/resourceConfig.json'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL for log config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' LOG CONFIG '{"yarn_am": "hdfs:///jubatus-on-yarn/test/am_log4j.xml", "jubatus_proxy": "hdfs:///jubatus-on-yarn/test/jp_log4j.xml", "jubatus_server": "hdfs:///jubatus-on-yarn/test/js_log4j.xml"}'
      """.stripMargin)

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.logConfigJsonOrPath shouldBe Some(Left("""{"yarn_am": "hdfs:///jubatus-on-yarn/test/am_log4j.xml", "jubatus_proxy": "hdfs:///jubatus-on-yarn/test/jp_log4j.xml", "jubatus_server": "hdfs:///jubatus-on-yarn/test/js_log4j.xml"}"""))
  }

  it should "recognize CREATE MODEL without log config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.logConfigJsonOrPath shouldBe None
  }

  it should "not recognize CREATE MODEL for log config without 'CONFIG'" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' LOG '{"yarn_am": "hdfs:///jubatus-on-yarn/test/am_log4j.xml", "jubatus_proxy": "hdfs:///jubatus-on-yarn/test/jp_log4j.xml", "jubatus_server": "hdfs:///jubatus-on-yarn/test/js_log4j.xml"}'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL with log config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' LOG CONFIG FILE 'file:///tmp/config/logConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.logConfigJsonOrPath shouldNot be(None)
    create.logConfigJsonOrPath.get.isRight shouldBe true
    create.logConfigJsonOrPath.get.right.get shouldBe new URI("file:///tmp/config/logConfig.json")
  }

  it should "recognize CREATE MODEL with illegal log config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    try{
      val result: Option[JubaQLAST] = parser.parse(
        """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' LOG CONFIG FILE '{"test": 123}'
      """.stripMargin)
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.URISyntaxException])
    }
  }

  it should "recognize CREATE MODEL with LOG CONFIG FILES(wrong query)" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' LOG CONFIG FILES 'file:///tmp/config/logConfig.json'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL without server config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.serverConfigJsonOrPath shouldBe None
  }

  it should "recognize CREATE MODEL for server config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER CONFIG '{"thread": 3}'
      """.stripMargin)
    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.serverConfigJsonOrPath shouldBe Some(Left("""{"thread": 3}"""))
  }

  it should "not recognize CREATE MODEL for server config without value" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER CONFIG
      """.stripMargin)
    result shouldBe (None)
  }

  it should "not recognize CREATE MODEL for server config without 'CONFIG'" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER '{"thread": 3}'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL with server config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER CONFIG FILE 'file:///tmp/config/serverConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.serverConfigJsonOrPath shouldNot be(None)
    create.serverConfigJsonOrPath.get.isRight shouldBe true
    create.serverConfigJsonOrPath.get.right.get shouldBe new URI("file:///tmp/config/serverConfig.json")
  }

  it should "recognize CREATE MODEL with illegal server config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    try{
      val result: Option[JubaQLAST] = parser.parse(
        """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER CONFIG FILE '{"test": 123}'
      """.stripMargin)
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.URISyntaxException])
    }
  }

  it should "recognize CREATE MODEL with SERVER CONFIG FILES(wrong query)" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' SERVER CONFIG FILES 'file:///tmp/config/serverConfig.json'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL without proxy config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.proxyConfigJsonOrPath shouldBe None
  }

  it should "recognize CREATE MODEL for proxy config" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY CONFIG '{"thread": 3}'
      """.stripMargin)
    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.proxyConfigJsonOrPath shouldBe Some(Left("""{"thread": 3}"""))
  }

  it should "not recognize CREATE MODEL for proxy config without value" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY CONFIG
      """.stripMargin)
    result shouldBe (None)
  }

  it should "not recognize CREATE MODEL for proxy config without 'CONFIG'" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    var result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY '{"thread": 3}'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL with proxy config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY CONFIG FILE 'file:///tmp/config/proxyConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.algorithm shouldBe "CLASSIFIER"
    create.modelName shouldBe "test1"
    create.labelOrId shouldBe Some(("label", "l"))
    create.featureExtraction shouldBe List((WildcardAnyParameter, "fex"))
    create.proxyConfigJsonOrPath shouldNot be(None)
    create.proxyConfigJsonOrPath.get.isRight shouldBe true
    create.proxyConfigJsonOrPath.get.right.get shouldBe new URI("file:///tmp/config/proxyConfig.json")
  }

  it should "recognize CREATE MODEL with illegal proxy config file" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    try{
      val result: Option[JubaQLAST] = parser.parse(
        """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY CONFIG FILE '{"test": 123}'
      """.stripMargin)
      fail()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.URISyntaxException])
    }
  }

  it should "recognize CREATE MODEL with PROXY CONFIG FILES(wrong query)" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"test": 123}' PROXY CONFIG FILES 'file:///tmp/config/proxyConfig.json'
      """.stripMargin)
    result shouldBe (None)
  }

  it should "recognize CREATE MODEL toString with XXX CONFIG" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"jubatusConfig":"jubatus"}'
      | RESOURCE CONFIG '{"resourceConfig":"resource"}'
      | LOG CONFIG '{"logConfig":"log"}'
      | SERVER CONFIG '{"serverConfig":"server"}'
      | PROXY CONFIG '{"proxyConfig":"proxy"}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.toString should include regex  """CreateModel(.*\{"jub\.\.\.tus"\},\{"res\.\.\.rce"\},\{"log\.\.\.log"\},\{"ser\.\.\.ver"\},\{"pro\.\.\.oxy"\}.*)""".r
  }

  it should "recognize CREATE MODEL toString without XXX CONFIG" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG '{"jubatusConfig":"jubatus"}'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.toString should include regex  """CreateModel(.*\{"jub\.\.\.tus"\},None,None,None,None.*)""".r
  }

  it should "recognize CREATE MODEL toString with XXX CONFIG FILE" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    // use single quotation
    val result: Option[JubaQLAST] = parser.parse(
      """
      |CREATE CLASSIFIER MODEL test1 (label: l) AS * WITH fex CONFIG FILE 'file:///tmp/config/jubatusConfig.json'
      | RESOURCE CONFIG FILE 'file:///tmp/config/resourceConfig.json'
      | LOG CONFIG FILE 'file:///tmp/config/logConfig.json'
      | SERVER CONFIG FILE 'file:///tmp/config/serverConfig.json'
      | PROXY CONFIG FILE 'file:///tmp/config/proxyConfig.json'
      """.stripMargin
    )

    result shouldNot be(None)
    val create = result.get.asInstanceOf[CreateModel]
    create.toString should include regex  "CreateModel(.*file:///tmp/config/jubatusConfig.json,file:///tmp/config/resourceConfig.json,file:///tmp/config/logConfig.json,file:///tmp/config/serverConfig.json,file:///tmp/config/proxyConfig.json.*)"
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
      |  WHERE fid % 2 = 0
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

  it should "recognize UPDATE WITH for uppercase" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train WITH '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val updateWith = result.get.asInstanceOf[UpdateWith]
    updateWith.modelName shouldBe "juba_model"
    updateWith.rpcName shouldBe "train"
    updateWith.learningData shouldBe """{"test": 123}"""
  }

  it should "recognize UPDATE WITH for lowercase" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train with '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val updateWith = result.get.asInstanceOf[UpdateWith]
    updateWith.modelName shouldBe "juba_model"
    updateWith.rpcName shouldBe "train"
    updateWith.learningData shouldBe """{"test": 123}"""
  }

  it should "recognize UPDATE WITH for mixedcase" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train With '{"test": 123}'
      """.stripMargin)

    result shouldNot be(None)
    val updateWith = result.get.asInstanceOf[UpdateWith]
    updateWith.modelName shouldBe "juba_model"
    updateWith.rpcName shouldBe "train"
    updateWith.learningData shouldBe """{"test": 123}"""
  }

  it should "not recognize UPDATE WITH without with" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train '{"test": 123}'
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize UPDATE WITH without learningData" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      UPDATE MODEL juba_model USING train WITH
      """.stripMargin)

    result should be(None)
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

  // TODO write more SAVE MODEL tests

  it should "recognize SAVE MODEL for Development Mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING "file:///home/data/models" AS test001
      """.stripMargin)

    result shouldNot be(None)
    val sm = result.get.asInstanceOf[SaveModel]
    sm.modelName shouldBe "test"
    sm.modelPath shouldBe """file:///home/data/models"""
    sm.modelId shouldBe "test001"
  }

  it should "recognize SAVE MODEL for Production Mode" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING "hdfs:///data/models" AS id
      """.stripMargin)

    result shouldNot be(None)
    val sm = result.get.asInstanceOf[SaveModel]
    sm.modelName shouldBe "test"
    sm.modelPath shouldBe """hdfs:///data/models"""
    sm.modelId shouldBe "id"
  }

  it should "not recognize SAVE MODEL without ModelName" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL USING "hdfs:///data/models" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize SAVE MODEL ModelName is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL "" USING "hdfs:///data/models" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize SAVE MODEL without ModelPath" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING  AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize SAVE MODEL ModelPath is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING "" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize SAVE MODEL without ModelId" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING "hdfs:///data/models" AS
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize SAVE MODEL ModelId is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      SAVE MODEL test USING "hdfs:///data/models" AS ""
      """.stripMargin)

    result should be(None)
  }

  // TODO write more LOAD MODEL tests

  it should "recognize LOAD MODEL Development Mode/file:scheme" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING "file:///home/data/models" AS test001
      """.stripMargin)

    result shouldNot be(None)
    val sm = result.get.asInstanceOf[LoadModel]
    sm.modelName shouldBe "test"
    sm.modelPath shouldBe """file:///home/data/models"""
    sm.modelId shouldBe "test001"
  }

  it should "recognize LOAD MODEL Production Mode/hdfs:scheme" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING "hdfs:///data/models" AS id
      """.stripMargin)

    result shouldNot be(None)
    val sm = result.get.asInstanceOf[LoadModel]
    sm.modelName shouldBe "test"
    sm.modelPath shouldBe """hdfs:///data/models"""
    sm.modelId shouldBe "id"
  }

  it should "not recognize LOAD MODEL without ModelName" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL USING "hdfs:///data/models" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize LOAD MODEL ModelName is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL "" USING "hdfs:///data/models" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize LOAD MODEL without ModelPath" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING  AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize LOAD MODEL ModelPath is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING "" AS test001
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize LOAD MODEL without ModelId" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING "hdfs:///data/models" AS
      """.stripMargin)

    result should be(None)
  }

  it should "not recognize LOAD MODEL ModelId is Empty" taggedAs (LocalTest) in {
    val parser = new JubaQLParser
    val result: Option[JubaQLAST] = parser.parse(
      """
      LOAD MODEL test USING "hdfs:///data/models" AS ""
      """.stripMargin)

    result should be(None)
  }
}
