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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.json.JsonRDDCopy
import org.scalatest._

class AggregateFunctionSpec
  extends FlatSpec
  with ShouldMatchers
  with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "AggregateFunctions")
  val sqlc = new SQLContext(sc)

  val doubleList = List((1L, (2L, 1.0)), (1L, (9L, 2.0)),
    (1L, (8L, 1.5)), (1L, (100L, 2.5)), (2L, (1L, 3.0)))
  val prettyDoubleInput = doubleList.groupBy(_._1).map(kv =>
    (kv._1, kv._2.map(_._2).sortBy(_._1).map(_._2))).toList.sortBy(_._1)
  val doubleRdd = sc.parallelize(doubleList)

  val stringList = List((1L, (2L, "a")), (1L, (9L, "b")),
    (1L, (8L, "a")), (1L, (100L, "a")), (2L, (1L, "c")))
  val prettyStringInput = stringList.groupBy(_._1).map(kv =>
    (kv._1, kv._2.map(_._2).sortBy(_._1).map(_._2))).toList.sortBy(_._1)
  val stringRdd = sc.parallelize(stringList)


  "avg" should "compute the average" taggedAs (LocalTest) in {
    val funObj = AvgFun
    val resultRdd = funObj.aggFun(doubleRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printDoubleInputResult(result)
    result(1).asInstanceOf[Double] shouldBe 1.75
    result(2).asInstanceOf[Double] shouldBe 3.0
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = AvgFun
    checkTypeResult(funObj, doubleRdd)
  }


  "stddev" should "compute the standard deviation" taggedAs (LocalTest) in {
    val funObj = StdDevFun
    val resultRdd = funObj.aggFun(doubleRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printDoubleInputResult(result)
    result(1).asInstanceOf[Double] shouldBe 0.55901699437495 +- 0.001
    result(2).asInstanceOf[Double] shouldBe 0.0
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = StdDevFun
    checkTypeResult(funObj, doubleRdd)
  }


  "quantile" should "compute the median" taggedAs (LocalTest) in {
    val funObj = new QuantileFun()
    val resultRdd = funObj.aggFun(doubleRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printDoubleInputResult(result)
    result(1).asInstanceOf[Double] shouldBe 2.0
    result(2).asInstanceOf[Double] shouldBe 3.0
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = new QuantileFun()
    checkTypeResult(funObj, doubleRdd)
  }


  "linapprox" should "compute the slope and intercept" taggedAs (LocalTest) in {
    val funObj = LinApproxFun
    val resultRdd = funObj.aggFun(doubleRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printDoubleInputResult(result)
    val firstResult = result(1).asInstanceOf[Map[String, Any]]
    firstResult("a") shouldBe 0.5
    firstResult("b") shouldBe 1.0
    val secondResult = result(2).asInstanceOf[Map[String, Any]]
    secondResult("a").asInstanceOf[Double].isNaN shouldBe true
    secondResult("b").asInstanceOf[Double].isNaN shouldBe true
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = LinApproxFun
    checkTypeResult(funObj, doubleRdd)
  }


  "fourier" should "compute the Fourier coefficients" taggedAs (LocalTest) ignore {
    val funObj = FourierCoeffsFun
    /* The code to compute Fourier coefficients internally uses
     * FastFourierTransformer.transformInPlace(data, DftNormalization.STANDARD, TransformType.INVERSE)
     * from Apache Commons Math 3.x. It remains a bit mysterious
     * what this method actually computes...
     */
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = FourierCoeffsFun
    checkTypeResult(funObj, doubleRdd)
  }


  "wavelet" should "compute the Haar wavelet coefficients" taggedAs (LocalTest) ignore {
    val funObj = WaveletCoeffsFun
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = WaveletCoeffsFun
    checkTypeResult(funObj, doubleRdd)
  }


  "histogram" should "compute the value distribution" taggedAs (LocalTest) in {
    val funObj = new HistogramFun(1.0, 2.0, 3)
    val resultRdd = funObj.aggFun(doubleRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printDoubleInputResult(result)
    val firstResult = result(1).asInstanceOf[Seq[Double]]
    firstResult.size shouldBe 3
    firstResult(0) shouldBe 0.0
    firstResult(1) shouldBe 0.5
    firstResult(2) shouldBe 0.5
    val secondResult = result(2).asInstanceOf[Seq[Double]]
    secondResult.size shouldBe 3
    secondResult(0) shouldBe 0.0
    secondResult(1) shouldBe 0.0
    secondResult(2) shouldBe 1.0
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = new HistogramFun(1.0, 2.0, 3)
    checkTypeResult(funObj, doubleRdd)
  }


  "concat" should "compute the concatenation" taggedAs (LocalTest) in {
    val funObj = new ConcatFun("")
    val resultRdd = funObj.aggFun(stringRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printStringInputResult(result)
    result(1).asInstanceOf[String] shouldBe "aaba"
    result(2).asInstanceOf[String] shouldBe "c"
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = new ConcatFun("")
    checkTypeResult(funObj, stringRdd)
  }


  "maxelem" should "compute the most frequeny element" taggedAs (LocalTest) in {
    val funObj = MaxElemFun
    val resultRdd = funObj.aggFun(stringRdd)
    val result: Map[Long, Any] = resultRdd.collect().toMap
    printStringInputResult(result)
    result(1).asInstanceOf[String] shouldBe "a"
    result(2).asInstanceOf[String] shouldBe "c"
  }

  it should "declare the correct type" taggedAs (LocalTest) in {
    val funObj = MaxElemFun
    checkTypeResult(funObj, stringRdd)
  }


  def printDoubleInputResult(result: Map[Long, Any]) = {
    result.size shouldBe 2
    prettyDoubleInput.foreach(kv => info(kv._2 + " => " + result(kv._1)))
  }

  def printStringInputResult(result: Map[Long, Any]) = {
    result.size shouldBe 2
    prettyStringInput.foreach(kv => info(kv._2 + " => " + result(kv._1)))
  }

  def checkTypeResult[T](funObj: SomeAggregateFunction[T], input: RDD[(Long, (Long, T))]) = {
    // what *should* be the output type?
    val declaredResultType = funObj.outType
    val resultRDD = funObj.aggFun(input)
    val exampleResult = resultRDD.take(1).head._2
    val wrappedResultRDD = resultRDD.map(row => Map("result" -> row._2))
    // schema inference is done as in JsonRDD.inferSchema()
    val inferredSchema = JsonRDDCopy.createSchema(
      wrappedResultRDD.map(JsonRDDCopy.allKeysWithValueTypes).reduce(_ ++ _)
    )
    // what does the schema inferrer claim the returned type to be?
    val inferredResultType = inferredSchema.fields
      .filter(_.name == "result").map(_.dataType).head
    info("%s should have type %s, inferred type was %s".format(exampleResult,
      declaredResultType, inferredResultType))
    (inferredResultType, declaredResultType) match {
      case (s1: StructType, s2: StructType) =>
        s1.fields should contain theSameElementsAs s2.fields
      case _ =>
        inferredResultType shouldBe declaredResultType
    }
  }

  override protected def afterAll() = {
    sc.stop()
  }
}
