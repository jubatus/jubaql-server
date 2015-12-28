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

import org.apache.spark.sql.catalyst.types.BooleanType
import org.apache.spark.sql.catalyst.{SqlLexical, SqlParser}
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.parsing.input.CharArrayReader._

// TODO: move these to a proper file.
// TODO: rename to better ones.
sealed trait FeatureFunctionParameters

case object WildcardAnyParameter extends FeatureFunctionParameters

case class WildcardWithPrefixParameter(prefix: String) extends FeatureFunctionParameters

case class WildcardWithSuffixParameter(suffix: String) extends FeatureFunctionParameters

case class NormalParameters(params: List[String]) extends FeatureFunctionParameters


class JubaQLParser extends SqlParser with LazyLogging {

  class JubaQLLexical(keywords: Seq[String]) extends SqlLexical(keywords) {
    case class CodeLit(chars: String) extends Token {
      override def toString = "$$"+chars+"$$"
    }

    // used for parsing $$-delimited code blocks
    protected lazy val codeDelim: Parser[String] = '$' ~ '$' ^^
      { case a ~ b => "$$" }

    protected lazy val stringWithoutCodeDelim: Parser[String] = rep1( chrExcept('$', EofCh) ) ^^
      { case chars => chars mkString "" }

    protected lazy val codeContents: Parser[String] = repsep(stringWithoutCodeDelim, '$') ^^
      { case words => words mkString "$" }

    override lazy val token: Parser[Token] =
      ( identChar ~ rep( identChar | digit ) ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
        | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
        case i ~ None    => NumericLit(i mkString "")
        case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
      }
        | '\'' ~ rep( chrExcept('\'', EofCh) ) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
        | '\"' ~ rep( chrExcept('\"', EofCh) ) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
        | codeDelim ~> codeContents <~ codeDelim ^^ { case chars => CodeLit(chars) }
        | EofCh ^^^ EOF
        | codeDelim ~> failure("unclosed code literal")
        | '\'' ~> failure("unclosed string literal")
        | '\"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character")
        )
  }

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
  protected lazy val LOG = Keyword("LOG")
  protected lazy val STORAGE = Keyword("STORAGE")
  protected lazy val STREAM = Keyword("STREAM")
  protected lazy val CONFIG = Keyword("CONFIG")
  protected lazy val numeric = Keyword("numeric")
  protected lazy val string = Keyword("string")
  protected lazy val boolean = Keyword("boolean")
  protected lazy val STATUS = Keyword("STATUS")
  protected lazy val SHUTDOWN = Keyword("SHUTDOWN")
  protected lazy val START = Keyword("START")
  protected lazy val STOP = Keyword("STOP")
  protected lazy val PROCESSING = Keyword("PROCESSING")
  protected lazy val FUNCTION = Keyword("FUNCTION")
  protected lazy val RETURNS = Keyword("RETURNS")
  protected lazy val LANGUAGE = Keyword("LANGUAGE")
  protected lazy val FEATURE = Keyword("FEATURE")
  protected lazy val TRIGGER = Keyword("TRIGGER")
  protected lazy val FOR = Keyword("FOR")
  protected lazy val EACH = Keyword("EACH")
  protected lazy val ROW = Keyword("ROW")
  protected lazy val EXECUTE = Keyword("EXECUTE")
  protected lazy val SLIDING = Keyword("SLIDING")
  protected lazy val WINDOW = Keyword("WINDOW")
  protected lazy val SIZE = Keyword("SIZE")
  protected lazy val ADVANCE = Keyword("ADVANCE")
  protected lazy val TIME = Keyword("TIME")
  protected lazy val TUPLES = Keyword("TUPLES")
  protected lazy val OVER = Keyword("OVER")
  protected lazy val SAVE = Keyword("SAVE")
  protected lazy val LOAD = Keyword("LOAD")
  protected lazy val RESOURCE = Keyword("RESOURCE")
  protected lazy val SERVER = Keyword("SERVER")
  protected lazy val PROXY = Keyword("PROXY")

  override val lexical = new JubaQLLexical(reservedWords)

  // we should allow some common column names that have are also known as keywords
  protected lazy val colIdent = (COUNT | TIME | STATUS | MODEL | GROUP |
    ORDER | ident)

  override lazy val baseExpression: PackratParser[Expression] =
    expression ~ "[" ~ expression <~ "]" ^^ {
      case base ~ _ ~ ordinal => GetItem(base, ordinal)
    } |
      TRUE ^^^ Literal(true, BooleanType) |
      FALSE ^^^ Literal(false, BooleanType) |
      cast |
      "(" ~> expression <~ ")" |
      function |
      "-" ~> literal ^^ UnaryMinus |
      colIdent ^^ UnresolvedAttribute | // was: ident
      "*" ^^^ Star(None) |
      literal

  override lazy val projection: Parser[Expression] =
    expression ~ (opt(AS) ~> opt(colIdent)) ^^ { // was: opt(ident)
      case e ~ None => e
      case e ~ Some(a) => Alias(e, a)()
    }

  protected lazy val streamIdent = ident

  protected lazy val modelIdent = ident

  protected lazy val funcIdent = ident

  // column_name column_type
  protected lazy val stringPairs: Parser[(String, String)] = {
    colIdent ~ (numeric | string | boolean) ^^ {
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
    CREATE ~ DATASOURCE ~> streamIdent ~ opt("(" ~ rep1sep(stringPairs, ",") ~ ")") ~
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

  protected lazy val createModel: Parser[JubaQLAST] = {
    val wildcardAny: Parser[FeatureFunctionParameters] = "*" ^^ {
      case _ =>
        WildcardAnyParameter
    }
    val wildcardWithPrefixParam: Parser[FeatureFunctionParameters] = ident <~ "*" ^^ {
      case prefix =>
        WildcardWithPrefixParameter(prefix)
    }
    val wildcardWithSuffixParam: Parser[FeatureFunctionParameters] = "*" ~> ident ^^ {
      case suffix =>
        WildcardWithSuffixParameter(suffix)
    }
    // wildcardWithSuffixParam is first.
    // If wildcardAny precedes, *_suffix always matches to wildcardAny.
    val wildcard: Parser[FeatureFunctionParameters] = wildcardWithSuffixParam | wildcardAny | wildcardWithPrefixParam

    val oneParameter: Parser[NormalParameters] = colIdent ^^ {
      case param =>
        NormalParameters(List(param))
    }
    // this may take one parameter. Should such behavior avoided?
    val moreThanOneParameters: Parser[FeatureFunctionParameters] = "(" ~> rep1sep(colIdent, ",") <~ ")" ^^ {
      case params =>
        NormalParameters(params)
    }

    val featureFunctionParameters: Parser[FeatureFunctionParameters] = wildcard | oneParameter | moreThanOneParameters

    val labelOrId: Parser[(String, String)] = "(" ~> ident ~ ":" ~ colIdent <~ ")" ^^ {
      case labelOrId ~ _ ~ value if labelOrId == "label" || labelOrId == "id" =>
        (labelOrId, value)
    }

    val paramsAndFunction: Parser[(FeatureFunctionParameters, String)] = featureFunctionParameters ~ opt(WITH ~> funcIdent) ^^ {
      case params ~ functionName =>
        (params, functionName.getOrElse("id"))
    }

    CREATE ~> jubatusAlgorithm ~ MODEL ~ modelIdent ~ opt(labelOrId) ~ AS ~
      rep1sep(paramsAndFunction, ",") ~ CONFIG ~ stringLit ~ opt(RESOURCE ~ CONFIG ~> stringLit) ~ opt(LOG ~ CONFIG ~> stringLit) ~
      opt(SERVER ~ CONFIG ~> stringLit) ~ opt(PROXY ~ CONFIG ~> stringLit) ^^ {
      case algorithm ~ _ ~ modelName ~ maybeLabelOrId ~ _ ~ l ~ _ ~ config ~ resourceConfig ~ logConfig ~ serverConfig ~ proxyConfig =>
        CreateModel(algorithm, modelName, maybeLabelOrId, l, config, resourceConfig, logConfig, serverConfig, proxyConfig)
    }
  }

  protected lazy val createStreamFromSelect: Parser[JubaQLAST] = {
    CREATE ~ STREAM ~> streamIdent ~ FROM ~ select ^^ {
      case streamName ~ _ ~ selectPlan =>
        CreateStreamFromSelect(streamName, selectPlan)
    }
  }

  protected lazy val createStreamFromAnalyze: Parser[JubaQLAST] = {
    CREATE ~ STREAM ~> streamIdent ~ FROM ~ analyzeStream ~ opt(AS ~> colIdent) ^^ {
      case streamName ~ _ ~ analyzePlan ~ newColumn =>
        CreateStreamFromAnalyze(streamName, analyzePlan, newColumn)
    }
  }

  protected lazy val createTrigger: Parser[JubaQLAST] = {
    CREATE ~ TRIGGER ~ ON ~> streamIdent ~ FOR ~ EACH ~ ROW ~ opt(WHEN ~> expression) ~ EXECUTE ~ function ^^ {
      case dsName ~ _ ~ _ ~ _ ~ condition ~ _ ~ expr =>
        CreateTrigger(dsName, condition, expr)
    }
  }

  protected lazy val createStreamFromSlidingWindow: Parser[JubaQLAST] = {
    val aggregation: Parser[(String, List[Expression], Option[String])] =
      (ident | AVG) ~ "(" ~ rep1sep(expression, ",") ~ ")" ~ opt(AS ~> colIdent) ^^ {
        case funcName ~ _ ~ parameters ~ _ ~ maybeAlias =>
          (funcName, parameters, maybeAlias)
      }
    val aggregationList = rep1sep(aggregation, ",")

    val filter: Parser[Expression] = WHERE ~ expression ^^ { case _ ~ e => e}
    val having: Parser[Expression] = HAVING ~> expression

    CREATE ~ STREAM ~> streamIdent ~ FROM ~ SLIDING ~ WINDOW ~
      "(" ~ SIZE ~ numericLit ~ ADVANCE ~ numericLit ~ (TIME | TUPLES) ~ ")" ~
      OVER ~ streamIdent ~ WITH ~ aggregationList ~ opt(filter) ~ opt(having) ^^ {
      case streamName ~ _ ~ _ ~ _ ~ _ ~ _ /* FROM SLIDING WINDOW ( SIZE */ ~
        size ~ _ /* ADVANCE */ ~ advance ~ windowType ~ _ /* ) */ ~
        _ /* OVER */ ~ source ~ _ /* WITH */ ~ funcSpecs ~ f ~ h =>
        // start from a table/stream with the given name
        val base = UnresolvedRelation(Seq(source), None)
        // apply the precondition
        val withFilter = f.map(f => Filter(f, base)).getOrElse(base)
        // select only the column that we use in the window.
        val allColumns = funcSpecs.map(_._2.last)
        val withProjection = Project(assignAliases(allColumns), withFilter)
        // NB. we have to add a Cast to the correct type in every column later,
        // after we have mapped function names to concrete functions.

        CreateStreamFromSlidingWindow(streamName, size.toInt, advance.toInt,
          windowType.toLowerCase, withProjection, funcSpecs,
          h)
    }
  }

  protected lazy val logStream: Parser[JubaQLAST] = {
    LOG ~ STREAM ~> streamIdent ^^ {
      case streamName =>
        LogStream(streamName)
    }
  }

  protected lazy val update: Parser[JubaQLAST] = {
    UPDATE ~ MODEL ~> modelIdent ~ USING ~ funcIdent ~ (FROM ~ streamIdent | WITH ~ stringLit) ^^ {
      case modelName ~ _ ~ rpcName ~ (fromOrWith ~ source) if fromOrWith.compareToIgnoreCase("FROM") == 0 =>
        Update(modelName, rpcName, source)
      case modelName ~ _ ~ rpcName ~ (fromOrWith ~ learningData) if fromOrWith.compareToIgnoreCase("WITH") == 0 =>
        UpdateWith(modelName, rpcName, learningData)
    }
  }

  protected lazy val analyze: Parser[JubaQLAST] = {
    ANALYZE ~> stringLit ~ BY ~ MODEL ~ modelIdent ~ USING ~ funcIdent ^^ {
      case data ~ _ ~ _ ~ modelName ~ _ ~ rpc =>
        Analyze(modelName, rpc, data)
    }
  }

  protected lazy val analyzeStream: Parser[Analyze] = {
    ANALYZE ~> streamIdent ~ BY ~ MODEL ~ modelIdent ~ USING ~ funcIdent ^^ {
      case source ~ _ ~ _ ~ modelName ~ _ ~ rpc =>
        Analyze(modelName, rpc, source)
    }
  }

  protected lazy val status: Parser[JubaQLAST] = {
    STATUS ^^ {
      case _ =>
        Status()
    }
  }

  protected lazy val shutdown: Parser[JubaQLAST] = {
    SHUTDOWN ^^ {
      case _ =>
        Shutdown()
    }
  }

  protected lazy val startProcessing: Parser[JubaQLAST] = {
    START ~ PROCESSING ~> streamIdent ^^ {
      case dsName =>
        StartProcessing(dsName)
    }
  }

  protected lazy val stopProcessing: Parser[JubaQLAST] = {
    STOP ~> PROCESSING ^^ {
      case _ =>
        StopProcessing()
    }
  }

  /** A parser which matches a code literal */
  def codeLit: Parser[String] =
    elem("code literal", _.isInstanceOf[lexical.CodeLit]) ^^ (_.chars)

  protected lazy val createFunction: Parser[JubaQLAST] = {
    CREATE ~ FUNCTION ~> funcIdent ~ "(" ~ repsep(stringPairs, ",") ~ ")" ~
      RETURNS ~ (numeric | string| boolean) ~ LANGUAGE ~ ident ~ AS ~ codeLit ^^ {
      case f ~ _ ~ args ~ _ ~ _ /*RETURNS*/ ~ retType ~ _ /*LANGUAGE*/ ~ lang ~
        _ /*AS*/ ~ body =>
        CreateFunction(f, args, retType, lang, body)
    }
  }

  protected lazy val createFeatureFunction: Parser[JubaQLAST] = {
    CREATE ~ FEATURE ~ FUNCTION ~> funcIdent ~ "(" ~ repsep(stringPairs, ",") ~ ")" ~
      LANGUAGE ~ ident ~ AS ~ codeLit ^^ {
      case f ~ _ ~ args ~ _ ~ _ /*LANGUAGE*/ ~ lang ~
        _ /*AS*/ ~ body =>
        CreateFeatureFunction(f, args, lang, body)
    }
  }

  protected lazy val createTriggerFunction: Parser[JubaQLAST] = {
    CREATE ~ TRIGGER ~ FUNCTION ~> funcIdent ~ "(" ~ repsep(stringPairs, ",") ~ ")" ~
      LANGUAGE ~ ident ~ AS ~ codeLit ^^ {
      case f ~ _ ~ args ~ _ ~ _ /*LANGUAGE*/ ~ lang ~
        _ /*AS*/ ~ body =>
        CreateTriggerFunction(f, args, lang, body)
    }
  }

  protected lazy val saveModel: Parser[JubaQLAST] = {
    SAVE ~ MODEL ~> modelIdent ~ USING ~ stringLit ~ AS ~ ident ^^ {
      case modelName ~ _ ~ modelPath ~ _ ~ modelId =>
        modelPath match {
          case "" =>
            null
          case _ =>
            SaveModel(modelName, modelPath, modelId)
        }
    }
  }

  protected lazy val loadModel: Parser[JubaQLAST] = {
    LOAD ~ MODEL ~> modelIdent ~ USING ~ stringLit ~ AS ~ ident ^^ {
      case modelName ~ _ ~ modelPath ~ _ ~ modelId =>
        modelPath match {
          case "" =>
            null
          case _ =>
            LoadModel(modelName, modelPath, modelId)
        }
    }
  }

  protected lazy val jubaQLQuery: Parser[JubaQLAST] = {
    createDatasource |
      createModel |
      createStreamFromSelect |
      createStreamFromSlidingWindow |
      createStreamFromAnalyze |
      createTrigger |
      logStream |
      update |
      analyze |
      status |
      shutdown |
      startProcessing |
      stopProcessing |
      createFunction |
      createFeatureFunction |
      createTriggerFunction |
      saveModel |
      loadModel
  }

  // note: apply cannot override incompatible type with parent class
  //override def apply(input: String): Option[JubaQLAST] = {
  def parse(input: String): Option[JubaQLAST] = {
    logger.info(s"trying to parse '$input'")
    phrase(jubaQLQuery)(new lexical.Scanner(input)) match {
      case Success(r, q) =>
        logger.debug(s"successfully parsed input: $r")
        Option(r)
      case x =>
        logger.warn(s"failed to parse input as JubaQL: $x")
        None
    }
  }
}
