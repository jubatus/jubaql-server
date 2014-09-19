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

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import com.typesafe.scalalogging.slf4j.LazyLogging

class JubaQLParser extends SqlParser with LazyLogging {

  protected lazy val CREATE = Keyword("CREATE")
  protected lazy val DATASOURCE = Keyword("DATASOURCE")
  protected lazy val MODEL = Keyword("MODEL")
  protected lazy val ANOMALY = Keyword("ANOMALY")
  protected lazy val CLASSIFIER = Keyword("CLASSIFIER")
  protected lazy val RECOMMENDER = Keyword("RECOMMENDER")
  protected lazy val WITH = Keyword("WITH")
  protected lazy val UPDATE = Keyword("UPDATE")
  protected lazy val ANALYZE = Keyword("ANALYZE")
  protected lazy val USING = Keyword("USING")
  protected lazy val DATA = Keyword("DATA")
  protected lazy val STORAGE = Keyword("STORAGE")
  protected lazy val STREAM = Keyword("STREAM")
  protected lazy val config = Keyword("config")
  protected lazy val numeric = Keyword("numeric")
  protected lazy val string = Keyword("string")
  protected lazy val boolean = Keyword("boolean")
  protected lazy val SHUTDOWN = Keyword("SHUTDOWN")
  protected lazy val STOP = Keyword("STOP")
  protected lazy val PROCESSING = Keyword("PROCESSING")

  // column_name column_type
  protected lazy val stringPairs: Parser[(String, String)] = {
    ident ~ (numeric | string | boolean) ^^ {
      case x ~ y => (x, y)
    }
  }

  protected lazy val stream: Parser[String] = {
    STREAM ~ ":" ~> stringLit ^^ {
      case url => url
    }
  }
  protected lazy val streamList: Parser[List[String]] = {
    "," ~> rep1sep(stream, ",") ^^ {
      case rep => rep
    }
  }

  // CREATE DATASOURCE source_name ( column_name data_type, [...]) FROM sink_id
  protected lazy val createDatasource: Parser[JubaQLAST] = {
    CREATE ~ DATASOURCE ~> ident ~ opt("(" ~ rep1sep(stringPairs, ",") ~ ")") ~
      FROM ~ "(" ~ STORAGE ~ ":" ~ stringLit ~ opt(streamList) <~ ")" ^^ {
      case sourceName ~ rep ~ _ /*FROM*/ ~ _ ~ _ /*STORAGE*/ ~ _ ~ storage ~ streams =>
        rep match {
          case Some(r) =>
            CreateDatasource(sourceName, r._1._2, storage, streams.getOrElse(List[String]()))
          case None =>
            CreateDatasource(sourceName, List(), storage, streams.getOrElse(List[String]()))
        }
    }
  }

  protected lazy val jubatusAlgorithm: Parser[String] = {
    (ANOMALY | CLASSIFIER | RECOMMENDER) ^^ {
      case x => x
    }
  }

  protected lazy val createWith: Parser[(String, List[String])] = {
    ident ~ ":" ~ stringLit ^^ {
      case key ~ _ ~ value =>
        (key, List(value))
    } |
      ident ~ ":" ~ "[" ~ rep1sep(stringLit, ",") <~ "]" ^^ {
        case key ~ _ ~ _ ~ values =>
          (key, values)
      }
  }

  // CREATE algorithm_name MODEL jubatus_name WITH config = "json string"
  protected lazy val createModel: Parser[JubaQLAST] = {
    CREATE ~> jubatusAlgorithm ~ MODEL ~ ident ~ WITH ~ "(" ~ opt(rep1sep(createWith, ",")) ~ ")" ~ "config" ~ "=" ~ stringLit ^^ {
      case algorithm ~ _ ~ modelName ~ _ /*with*/ ~ _ ~ cwith ~ _ ~ _ /*config*/ ~ _ ~ config =>
        CreateModel(algorithm, modelName, config, cwith.getOrElse(List[(String, List[String])]()))
    }
  }

  // This select copied from SqlParser, and removed `from` clause.
  protected lazy val jubaqlSelect: Parser[LogicalPlan] =
    SELECT ~> opt(DISTINCT) ~ projections ~
      opt(filter) ~
      opt(grouping) ~
      opt(having) ~
      opt(orderBy) ~
      opt(limit) <~ opt(";") ^^ {
      case d ~ p ~ f ~ g ~ h ~ o ~ l =>
        val base = NoRelation
        val withFilter = f.map(f => Filter(f, base)).getOrElse(base)
        val withProjection =
          g.map {
            g =>
              Aggregate(g, assignAliases(p), withFilter)
          }.getOrElse(Project(assignAliases(p), withFilter))
        val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
        val withHaving = h.map(h => Filter(h, withDistinct)).getOrElse(withDistinct)
        val withOrder = o.map(o => Sort(o, withHaving)).getOrElse(withHaving)
        val withLimit = l.map {
          l => Limit(l, withOrder)
        }.getOrElse(withOrder)
        withLimit
    }

  protected lazy val update: Parser[JubaQLAST] = {
    UPDATE ~ MODEL ~> ident ~ USING ~ ident ~ FROM ~ ident ^^ {
      case modelName ~ _ ~ rpcName ~ _ ~ source =>
        Update(modelName, rpcName, source)
    }
  }

  protected lazy val analyze: Parser[JubaQLAST] = {
    ANALYZE ~> stringLit ~ BY ~ MODEL ~ ident ~ USING ~ ident ^^ {
      case data ~ _ ~ _ ~ modelName ~ _ ~ rpc =>
        Analyze(modelName, rpc, data)
    }
  }

  protected lazy val shutdown: Parser[JubaQLAST] = {
    SHUTDOWN ^^ {
      case _ =>
        Shutdown()
    }
  }

  protected lazy val stopProcessing: Parser[JubaQLAST] = {
    STOP ~> PROCESSING ^^ {
      case _ =>
        StopProcessing()
    }
  }

  protected lazy val jubaQLQuery: Parser[JubaQLAST] = {
    createDatasource |
      createModel |
      update |
      analyze |
      shutdown |
      stopProcessing
  }

  // note: apply cannot override incompatible type with parent class
  //override def apply(input: String): Option[JubaQLAST] = {
  def parse(input: String): Option[JubaQLAST] = {
    phrase(jubaQLQuery)(new lexical.Scanner(input)) match {
      case Success(r, q) =>
        logger.debug(s"successfully parsed '$input' into $r")
        Option(r)
      case x =>
        logger.warn(s"failed to parse '$input' as JubaQL")
        None
    }
  }
}
