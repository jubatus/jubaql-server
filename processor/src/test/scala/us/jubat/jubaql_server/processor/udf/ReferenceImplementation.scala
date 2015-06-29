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
import scala.util.Random
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.commons.math3.transform.DftNormalization
import org.apache.commons.math3.transform.FastFourierTransformer
import org.apache.commons.math3.transform.TransformType
import scala.collection.mutable.LinkedHashMap

/**
 * 参照実装です。
 * 計算量を度外視し確実に動くことを重視します。
 */
object ReferenceImplementation {
  import math._
  def nextPowerOf2(value: Int) = pow(2, ceil(log(value) / log(2))).toInt

  def sortByIndex[_Index: Ordering, _Value](values: Seq[(_Index, _Value)]): Seq[(_Index, _Value)] = values.sortBy(_._1)
  def stripIndex[_Index: Ordering, _Value](values: Seq[(_Index, _Value)]): Seq[_Value] = values.map { case (_, value) => value }
  def shuffle[_Index, _Value](values: Seq[(_Index, _Value)]): Seq[(_Index, _Value)] = Random.shuffle(values)

  def average(values: Seq[Double]): Double = new Mean().evaluate(values.toArray)

  /**
   * アルゴリズムの差による∞とNaNの取り扱いを確認した後に、commons-mathで値を計算します。
   */
  def standardDeviation(values: Seq[Double]): Double = {
    val meanSquared = pow(values.sum / values.size, 2)
    val squaredMean = values.map(pow(_, 2)).sum / values.size
    sqrt(squaredMean - meanSquared)
  }

  /**
   * ■ 数が合わない時は最も近い値を取る
   * ■ 候補が2つある場合は大きい値を取る
   */
  def quantile(values: Seq[Double], position: Double): Double = position match {
    case 0d => values.min
    case position => {
      val index = ((values.length - 1) * position).round.toInt
      values.sorted.iterator.drop(index).next
    }
  }

  def regressionline(values: Seq[Double]): (Double, Double) = {
    val regression = new SimpleRegression()
    values.zipWithIndex.foreach { case (value, index) => regression.addData(index, value) }
    (regression.getSlope(), regression.getIntercept())
  }

  def fourierCoefficients(values: Seq[Double]): (Seq[Double], Seq[Double]) = {
    val data = Array.ofDim[Double](2, nextPowerOf2(values.length))
    values.copyToArray(data(0))
    FastFourierTransformer.transformInPlace(data, DftNormalization.STANDARD, TransformType.INVERSE)
    (data(0), data(1))
  }

  def waveletCoefficients(values: Seq[Double]): Seq[Double] = {
    val data = Array.ofDim[Double](nextPowerOf2(values.length))
    values.copyToArray(data)
    WaveletCoeffsFun.transform(data)
  }

  /**
   * <code>
   * lowestUpperBound = 0,  highestLowerBound = 100, numBins = 7 のとき
   * ビン幅は 20
   * 各ビンの上限、下限は
   * bin 0 [ -∞ 〜   0)
   * bin 1 [  0 〜  20)
   * bin 2 [ 20 〜  40)
   * bin 3 [ 40 〜  60)
   * bin 4 [ 60 〜  80)
   * bin 5 [ 80 〜 100)
   * bin 6 [100 〜 +∞)
   * </code>
   */
  def histogram(values: Seq[Double], lowestUpperBound: Double, highestLowerBound: Double, numBins: Int): Seq[Double] = {
    val bounds = numBins match {
      case 0 => throw new IllegalArgumentException(s"numBins(=${numBins}) should be >=1")
      case 1 => Seq(Double.NegativeInfinity, Double.PositiveInfinity)
      case 2 => {
        if (lowestUpperBound == highestLowerBound)
          Seq(Double.NegativeInfinity, lowestUpperBound, Double.PositiveInfinity)
        else
          throw new IllegalArgumentException(s"if numBins == 2 then lowestUpperBound(=${lowestUpperBound}) should equal highestLowerBound(=${highestLowerBound})")
      }
      case _ =>
        val rangeWidth = highestLowerBound - lowestUpperBound
        val rangeBins = numBins - 2
        val width = rangeWidth / rangeBins
        Double.NegativeInfinity +: (0 to rangeBins).map(_ * width + lowestUpperBound) :+ Double.PositiveInfinity
    }

    case class Bin(lowerBound: Double, upperBound: Double, var count: Int) { def contains(value: Double) = lowerBound <= value && value < upperBound }
    val bins = bounds.sliding(2).map { case Seq(lowerBound, upperBound) => Bin(lowerBound, upperBound, 0) }.toSeq

    var size = 0
    values.foreach { value =>
      size += 1
      bins.find(_.contains(value)) match {
        case Some(bin) => bin.count += 1
        case None      => ()
      }
    }

    bins.map(_.count.toDouble / size)
  }

  def concatenate(values: Seq[String], separator: String): String = {
    if (separator == null) throw new IllegalArgumentException
    values.foldLeft(new StringBuilder) {
      case (a, v) => v match {
        case null => throw new NullPointerException
        case ""   => a
        case v    => (if (a.length > 0) a.append(separator) else a).append(v)
      }
    }.toString
  }

  def mode[_Value: Ordering](values: Seq[_Value]): _Value = {
    require(!values.isEmpty)

    case class Counter(var count: Int)

    values.foldLeft(LinkedHashMap[_Value, Counter]()) {
      case (counters, value) =>
        if (value == null)
          throw new NullPointerException
        else
          counters.get(value) match {
            case Some(counter) => counter.count += 1
            case None          => counters.put(value, Counter(1))
          }
        counters
    }.toSeq.reverse.maxBy { case (value, counter) => counter.count }._1
  }
}
