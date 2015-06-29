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
package us.jubat.jubaql_server.processor.udf

import scala.math.Ordering

import org.apache.spark.SharedSparkContext
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import com.typesafe.scalalogging.slf4j.LazyLogging

import OrderedValueRDDFunctions.rddToOrderedValueRDDFunctions

case class Int0to7(val value: Int) extends AnyVal
case class Int0to100(val value: Int) extends AnyVal
case class Double0to1(val value: Double) extends AnyVal
case class Int1to4(val value: Int) extends AnyVal

class AggregateFunctionsTest extends FlatSpec
  with Matchers
  with GeneratorDrivenPropertyChecks
  with SharedSparkContext
  with LazyLogging {
  import scala.language.implicitConversions

  val numSlices = 2
  implicit def convertSeqToRdd[_Key, _Value](in: Seq[(_Key, _Value)]): RDD[(_Key, _Value)] = sc.parallelize(in, numSlices)
  implicit def convertDouble0to1ToDouble(in: Double0to1): Double = in.value
  implicit def convertInt1to4ToInt(in: Int1to4): Int = in.value

  import org.apache.spark.SparkContext._
  import org.scalatest.concurrent.Timeouts._
  import org.scalacheck.Arbitrary._

  /** 任意の Seq[(Int, Double)] を生成するためのジェネレータです。 */
  implicit val arbIndexedValues = Arbitrary {
    for {
      values <- arbitrary[Seq[Double]]
      if !values.isEmpty
    } yield values.zipWithIndex.map { case (value, index) => (index, value) }
  }

  /** [0 - 7] の値域をもつ任意の Int を生成するためのジェネレータです。 */
  implicit val arbInt0to7 = Arbitrary {
    for {
      value <- Gen.choose(0, 7)
    } yield Int0to7(value)
  }

  /** [1 - 4] の値域をもつ任意の Int を生成するためのジェネレータです。 */
  implicit val arbRepartition = Arbitrary {
    for {
      value <- Gen.choose(1, 4)
    } yield Int1to4(value)
  }

  /** [0 - 100] の値域をもつ任意の Int を生成するためのジェネレータです。 */
  implicit val arbInt0to100 = Arbitrary {
    for {
      value <- Gen.choose(0, 100)
    } yield Int0to100(value)
  }

  /** [0.0 - 1.0] の値域をもつ任意の Double を生成するためのジェネレータです。 */
  implicit val arbDouble0to1 = Arbitrary {
    for {
      value <- Gen.choose(0d, 1d)
    } yield Double0to1(value)
  }

  /** actural と expected の各要素を check 関数を使って比較します。 */
  def test[_Key, _Result, _Index: Ordering, _Value](expected: Seq[(_Key, (_Index, _Value))], actual: Seq[(_Key, _Result)])(check: (Seq[(_Index, _Value)], _Result) => Unit) = {
    actual.foreach { case (key, result) => check(expected.filter(_._1 == key).map(_._2), result) }
  }

  "OrderedValueRDDFunctions.groupByKeyAndSortValues" should "sort values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))]) =>
      test(values, sc.parallelize(values, 2).groupByKeyAndSortValues(identity(_)).mapValues(_.toList).collect()) {
        case (values, result) => result should be(values.sorted)
      }
    }
  }

  it should "be escalate exception" in {
    an[SparkException] should be thrownBy sc.parallelize(Seq((0, (0, Double.NaN))), 2).groupByKeyAndSortValues(_ => throw new IllegalArgumentException).collect()
  }

  /**
   * left と right を誤差を考慮して比較します。
   * 既存の plusOrMinus を使った比較では、巨大な値の比較で不都合があったため独自に実装しました。
   */
  def equalWithinTolerance(left: Double, right: Double): Boolean = {
    if (left == right) true
    else if (left.isNaN && right.isNaN) true
    else if (left.isInfinity || right.isInfinity) false
    else (left - right).abs / (left.abs max right.abs) <= 1E-15
  }

  /** left と right を誤差を考慮して比較します。 */
  def equalWithinTolerance(left: Seq[Double], right: Seq[Double]): Boolean = {
    left.length == right.length && (left zip right).foldLeft(true) { case (a, (l, r)) => a && equalWithinTolerance(l, r) }
  }

  /** left と right を誤差を考慮して比較する Matcher です。 */
  def beEqualWithinTolerance(right: Double) = Matcher {
    (left: Double) =>
      MatchResult(
        equalWithinTolerance(left, right),
        s"${left} was not equal to ${right}",
        "was equal to"
      )
  }

  /** left と right を誤差を考慮して比較する Matcher です。 */
  def beEqualWithinTolerance(right: => (Double, Double)) = Matcher {
    (left: (Double, Double)) =>
      MatchResult(
        equalWithinTolerance(left._1, right._1) && equalWithinTolerance(left._2, right._2),
        s"${left} was not equal to ${right}",
        "was equal to"
      )
  }

  /** left と right を誤差を考慮して比較する Matcher です。 */
  def beEqualWithinTolerance(right: Seq[Double]) = Matcher {
    (left: Seq[Double]) =>
      MatchResult(
        equalWithinTolerance(left, right),
        s"${left} was not equal to ${right}",
        "was equal to"
      )
  }

  /** left と right を誤差を考慮して比較する Matcher です。 */
  def beEqualWithinTolerance(right: (Seq[Double], Seq[Double])) = Matcher {
    (left: (Seq[Double], Seq[Double])) =>
      MatchResult(
        equalWithinTolerance(left._1, right._1) && equalWithinTolerance(left._2, right._2),
        s"${left} was not equal to ${right}",
        "was equal to"
      )
  }

  "beEqualWithinTolerance" should "compare numbers" in {
    1d should not be beEqualWithinTolerance(0d)
    1.00001d should not be beEqualWithinTolerance(1.00002d)

    (+0d) should beEqualWithinTolerance(-0d)
    1d should beEqualWithinTolerance(1d)
    1E-300 should beEqualWithinTolerance(1E-300)
    1E300 should beEqualWithinTolerance(1E300)
    (1d, 1d) should beEqualWithinTolerance((1d, 1d))
    Seq(1d) should beEqualWithinTolerance(Seq(1d))
    (Seq(1d), Seq(1d)) should beEqualWithinTolerance((Seq(1d), Seq(1d)))
    -7.034414131033201E231 should beEqualWithinTolerance(-7.0344141310332E231)
    Seq(-7.034414131033201E231) should beEqualWithinTolerance(Seq(-7.0344141310332E231))
  }

  it should "compare Infinite or NaN" in {
    Double.NaN should beEqualWithinTolerance(Double.NaN)
    Double.PositiveInfinity should beEqualWithinTolerance(Double.PositiveInfinity)
    Double.NegativeInfinity should beEqualWithinTolerance(Double.NegativeInfinity)
  }

  "Average.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, AvgFun(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.average(values.map(_._2)))
      }
    }
  }

  it should "return empty seq when input empty" in {
    AvgFun(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Long, Double)]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy AvgFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  "StdDevFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, StdDevFun(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.standardDeviation(values.map(_._2)))
      }
    }
  }

  it should "return empty seq when input empty" in {
    StdDevFun(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Long, Double)]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy StdDevFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  "QuantileFun" should "throw exception when construct with invalid parameter" in {
    an[IllegalArgumentException] should be thrownBy new QuantileFun(-1E-15)
    an[IllegalArgumentException] should be thrownBy new QuantileFun(1.0 + 1E-15)
    an[IllegalArgumentException] should be thrownBy new QuantileFun(Double.NaN)
    an[IllegalArgumentException] should be thrownBy new QuantileFun(Double.PositiveInfinity)
    an[IllegalArgumentException] should be thrownBy new QuantileFun(Double.NegativeInfinity)
  }

  "QuantileFun.apply" should "return correct values" in {
    forAll { (position: Double0to1, values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, new QuantileFun(position)(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.quantile(values.sorted.map(_._2), position))
      }
    }
  }

  it should "return empty seq when input empty" in {
    forAll { (position: Double0to1) =>
      new QuantileFun(position)(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Long, Double)]())
    }
  }

  it should "throw exception when invalid input" in {
    forAll { (position: Double0to1) =>
      an[IllegalArgumentException] should be thrownBy new QuantileFun(position)(null.asInstanceOf[RDD[(Int, (Long, Double))]])
    }
  }

  "LinApproxFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, LinApproxFun(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.regressionline(values.sorted.map(_._2)))
      }
    }
  }

  it should "return empty seq when input empty" in {
    LinApproxFun(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Int, (Double, Double))]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy LinApproxFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  "FourierCoeffsFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, FourierCoeffsFun(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.fourierCoefficients(values.sorted.map(_._2)))
      }
    }
  }

  it should "return empty seq when input empty" in {
    FourierCoeffsFun(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Int, (Seq[Double], Seq[Double]))]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy FourierCoeffsFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  "WaveletCoeffsFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], partition: Int1to4) =>
      test(values, WaveletCoeffsFun(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.waveletCoefficients(values.sorted.map(_._2)))
      }
    }
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy WaveletCoeffsFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  it should "transform short length values" in {
    WaveletCoeffsFun(Seq[(Int, (Long, Double))]()).collect().size should be(0)
    WaveletCoeffsFun(Seq((1, (1, 4d)))).collect().find(_._1 == 1).get._2 should be(Seq(4d))
  }

  it should "be fast" in {
    import org.scalatest.time.SpanSugar._
    val data = for {
      key <- (1 to 128)
      value <- (0 to 65535)
    } yield (key, (value, value.toDouble))

    failAfter(8 seconds) {
      WaveletCoeffsFun(sc.parallelize(data, 4))
    }
  }

  "HistogramFun" should "throw exception when construct with invalid parameter" in {
    an[IllegalArgumentException] should be thrownBy new HistogramFun(0.1d, 0.9d, -1)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(0.1d, 0.9d, 0)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(0.1d, 0.9d, 1)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(Double.NegativeInfinity, Double.PositiveInfinity, 1)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(Double.PositiveInfinity, Double.PositiveInfinity, 1)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(Double.NegativeInfinity, Double.NegativeInfinity, 1)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(0.1d, 0.9d, 2)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(0d, 0d, 3)
    an[IllegalArgumentException] should be thrownBy new HistogramFun(1d, 0d, 3)
  }

  "HistogramFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, Double))], bound1: Double0to1, bound2: Double0to1, num: Int0to100, partition: Int1to4) =>
      val numBins = num.value + 1

      val (lowestUpperBound, highestLowerBound) = numBins match {
        case 1 => (Double.PositiveInfinity, Double.NegativeInfinity)
        case 2 => (bound1.value, bound1.value)
        case _ => (math.min(bound1, bound2), math.max(bound1, bound2))
      }

      test(values, new HistogramFun(lowestUpperBound, highestLowerBound, numBins)(values.repartition(partition)).collect()) {
        case (values, result) => result should beEqualWithinTolerance(ReferenceImplementation.histogram(values.sorted.map(_._2), lowestUpperBound, highestLowerBound, numBins))
      }
    }
  }

  it should "return empty seq when input empty" in {
    new HistogramFun()(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Int, (Double, Double))]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy new HistogramFun()(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }

  "ConcatFun" should "throw exception when construct with invalid parameter" in {
    an[IllegalArgumentException] should be thrownBy new ConcatFun(null)
  }

  "ConcatFun.apply" should "return correct values" in {
    forAll { (values: Seq[(Int0to7, (Int, String))], separator: String, partition: Int1to4) =>
      test(values, new ConcatFun(separator)(values.repartition(partition)).collect()) {
        case (values, result) => result should be(ReferenceImplementation.concatenate(values.sorted.map(_._2), separator))
      }
    }
  }

  it should "return empty seq when input empty" in {
    new ConcatFun()(Seq[(Int, (Long, String))]()).collect() should be(Seq[(Long, String)]())
    forAll { (separator: String) =>
      new ConcatFun(separator)(Seq[(Int, (Long, String))]()).collect() should be(Seq[(Long, String)]())
    }
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy new ConcatFun()(null.asInstanceOf[RDD[(Int, (Long, String))]])
    forAll { (separator: String) =>
      an[IllegalArgumentException] should be thrownBy new ConcatFun(separator)(null.asInstanceOf[RDD[(Int, (Long, String))]])
    }
  }

  "MaxElemFun.apply" should "return correct values" in {
    MaxElemFun(Seq((1, (3, "f")), (1, (2, "m")), (1, (4, "m")), (1, (1, "f"))).repartition(4)).collect() should be(Seq((1, "m")))
    MaxElemFun(Seq((1, (2, "m")), (1, (3, "f")), (1, (1, "f")), (1, (4, "m"))).repartition(1)).collect() should be(Seq((1, "m")))

    forAll { (keyValues: Seq[(Int0to7, Double)], partition: Int1to4) =>
      val values = keyValues.zipWithIndex.map { case ((key, value), index) => (key, (index, value)) }
      test(values, MaxElemFun(values.repartition(partition)).collect()) {
        case (values, result) => result should be(ReferenceImplementation.mode(values.sortBy(_._1).map(_._2)))
      }
    }
  }

  it should "return empty seq when input empty" in {
    MaxElemFun(Seq[(Int, (Long, Double))]()).collect() should be(Seq[(Long, Double)]())
  }

  it should "throw exception when invalid input" in {
    an[IllegalArgumentException] should be thrownBy MaxElemFun(null.asInstanceOf[RDD[(Int, (Long, Double))]])
  }
}
