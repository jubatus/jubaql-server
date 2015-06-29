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

import scala.annotation.tailrec
import scala.math.Ordering
import scala.reflect.ClassTag

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.typesafe.scalalogging.slf4j.LazyLogging

import us.jubat.jubaql_server.processor.udf.OrderedValueRDDFunctions.rddToOrderedValueRDDFunctions

/**
 * 数値処理用関数の基底クラスです。
 */
trait DoubleValueWithIndexFunction extends LazyLogging {
  protected def checkValue[_Index](valueWithIndex: (_Index, Double)) = {
    if (valueWithIndex._2.isInfinity) {
      val message = s"${valueWithIndex}'s value is Infinity"
      logger.warn(message)
      throw new IllegalArgumentException(message)
    } else if (valueWithIndex._2.isNaN) {
      val message = s"${valueWithIndex}'s value is NaN"
      logger.warn(message)
      throw new IllegalArgumentException(message)
    } else
      valueWithIndex
  }

  import math._
  protected def nextPowerOf2(value: Int) = pow(2, ceil(log(value) / log(2))).toInt
}

/**
 * 文字列処理用関数の基底クラスです。
 */
trait StringValueWithIndexFunction extends LazyLogging {
  protected def checkValue[_Index: Ordering](valueWithIndex: (_Index, String)) = valueWithIndex match {
    case (index, null) =>
      val message = s"${valueWithIndex}'s text is null"
      logger.warn(message)
      throw new NullPointerException(message)
    case other => other
  }
}

/**
 * 入力窓内の要素の平均値(average)を返す関数です。
 */
object AvgFun extends DoubleValueWithIndexFunction {
  /**
   * 入力窓内の要素の key 毎に value の平均値を返します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, result) key: 集約キー, result: 計算結果値です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, Double)] = {
    require(rdd != null, "rdd is null")
    case class Aggregate(var sum: Double, var count: Int)
    rdd.aggregateByKey(Aggregate(0d, 0))(
      (aggregate, valueWithIndex) => {
        val value = checkValue(valueWithIndex)._2
        aggregate.sum += value
        aggregate.count += 1
        aggregate
      },
      (left, right) => { left.sum += right.sum; left.count += right.count; left }
    ).mapValues(aggregate => aggregate.sum / aggregate.count)
  }
}

/**
 * 入力窓内の要素の標準偏差値(standard deviate)を返す関数です。
 */
object StdDevFun extends DoubleValueWithIndexFunction {
  /**
   * 入力窓内の要素の key 毎に value の標準偏差値を返します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, result) key: 集約キー, result: 計算結果値です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, Double)] = {
    require(rdd != null, "rdd is null")
    case class Aggregate(var sum: Double, var squaredSum: Double, var count: Int)
    rdd.aggregateByKey(Aggregate(0d, 0d, 0))(
      (aggregate, valueWithIndex) => { val value = checkValue(valueWithIndex)._2; aggregate.sum += value; aggregate.squaredSum += math.pow(value, 2); aggregate.count += 1; aggregate },
      (left, right) => { left.sum += right.sum; left.squaredSum += right.squaredSum; left.count += right.count; left }
    ).mapValues(aggregate =>
        aggregate.count match {
          case count =>
            val meanSquared = math.pow(aggregate.sum / count, 2)
            val squaredMean = aggregate.squaredSum / count
            math.sqrt(squaredMean - meanSquared)
        }
      )
  }
}

/**
 * 入力窓内の要素の分位値(quantile)を返す関数です。
 *
 * @constructor 分位を指定して処理関数を生成します。
 * @param position 分位 [0 〜 1]。0のときは最小値、0.5のときは中央値、1のときは最大値をそれぞれ返します。
 * @throws IllegalArgumentException position < 0 または position > 1 のとき
 */
class QuantileFun(position: Double = 0.5d) extends DoubleValueWithIndexFunction with Serializable {
  require(position >= 0d, "position < 0")
  require(position <= 1d, "position > 1")

  /**
   * 入力窓内の要素の key 毎に value の分位値を返します。
   * 数が合わない時は最も近い値を取ります。
   * 候補が2つあるときは大きい値を取ります。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, value) key: 集約キー, value 分位値です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, Double)] = {
    require(rdd != null, "rdd is null")
    import org.apache.commons.math3.stat.descriptive.rank.Percentile
    implicit val valueOrdering: Ordering[(_Index, Double)] = new Ordering[(_Index, Double)] { override def compare(left: (_Index, Double), right: (_Index, Double)) = left._2.compare(right._2) }
    rdd.groupByKeyAndSortValues(checkValue).mapValues {
      values =>
        val index = ((values.size - 1) * position).round.toInt
        values.iterator.drop(index).next()._2
    }
  }
}

/**
 * 入力窓内の要素を最小2乗法で直線近似したときの傾き(slope)と切片(intercept)を返す関数です。
 */
object LinApproxFun extends DoubleValueWithIndexFunction {
  /**
   * 入力窓内の要素の key 毎に value の slope と intercept を返します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, (slope, intercept)) key: 集約キー, slope: 傾き, intercept: 切片です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index: Ordering](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, (Double, Double))] = {
    require(rdd != null, "rdd is null")
    import org.apache.commons.math3.stat.regression.SimpleRegression
    rdd.groupByKeyAndSortValues(checkValue).mapValues {
      values =>
        val regression = new SimpleRegression()
        values.zipWithIndex.foreach { case ((_, value), index) => regression.addData(index, value) }
        (regression.getSlope(), regression.getIntercept())
    }
  }
}

/**
 * 入力窓内の要素を離散フーリエ変換したときの係数(coefficient)を返す関数です。
 */
object FourierCoeffsFun extends DoubleValueWithIndexFunction {
  /**
   * 入力窓内の要素の key 毎に value を離散フーリエ変換したときの coefficient を返します。
   * もし rdd の key 毎の要素数が2のn乗でないときは最小の2のn乗まで要素の後ろにゼロ詰めを行ってから処理します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, coefficients) key: 集約キー, coefficients: 係数です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index: Ordering](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, (Seq[Double], Seq[Double]))] = {
    require(rdd != null, "rdd is null")

    import org.apache.commons.math3.transform.FastFourierTransformer
    import org.apache.commons.math3.transform.DftNormalization
    import org.apache.commons.math3.transform.TransformType

    rdd.groupByKeyAndSortValues(checkValue).mapValues {
      values =>
        val size = nextPowerOf2(values.size)
        val data = Array.ofDim[Double](2, size)
        values.toSeq.map(_._2).copyToArray(data(0))
        FastFourierTransformer.transformInPlace(data, DftNormalization.STANDARD, TransformType.INVERSE)
        (data(0), data(1))
    }
  }
}

/**
 * 入力窓内の要素をHaar離散ウェーブレット変換したときの係数(coefficient)を返す関数です。
 * もし input の要素数が2のn乗でないときは最小の2のn乗まで要素の後ろにゼロ詰めを行ってから処理します。
 */
object WaveletCoeffsFun extends DoubleValueWithIndexFunction {
  /**
   * 入力窓内の要素の key 毎に value をHaar離散ウェーブレット変換したときの coefficient を返します。
   * もし rdd の key 毎の要素数が2のn乗でないときは最小の2のn乗まで要素の後ろにゼロ詰めを行ってから処理します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, coefficients) key: 集約キー, coefficients: 係数です。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index: Ordering](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, Seq[Double])] = {
    require(rdd != null, "rdd is null")
    rdd.groupByKeyAndSortValues(checkValue).mapValues {
      values =>
        val data = nextPowerOf2(values.size) match {
          case size if size == values.size =>
            values.toArray.map(_._2)
          case size =>
            val newValues = new Array[Double](size)
            values.toSeq.map(_._2).copyToArray(newValues)
            newValues
        }
        transform(data)
    }
  }

  /**
   * Haar離散ウェーブレット変換を行います。
   * @param values 計算対象値です。要素数が2のn乗となっていることを期待します。
   * @return  係数です。
   */
  def transform(values: Seq[Double]): Seq[Double] = {
    @tailrec def transform(values: Seq[Double], coefficients: Seq[Seq[Double]]): Seq[Seq[Double]] = {
      def +/(index: Int) = (values(2 * index) + values(2 * index + 1)) / 2
      def -/(index: Int) = (values(2 * index) - values(2 * index + 1)) / 2
      values.length / 2 match {
        case 0 => coefficients ++ Seq(values)
        case 1 => coefficients ++ Seq(Seq(-/(0)), Seq(+/(0)))
        case length =>
          val indices = values.indices.dropRight(length)
          transform(indices.map(+/), coefficients ++ Seq(indices.map(-/)))
      }
    }
    transform(values, Seq()).reverse.flatten
  }
}

/**
 * 入力窓内の要素のヒストグラム(histogram)を返す関数です。
 * たとえば、lowestUpperBound = 0.1,  highestLowerBound = 0.9, numBins = 10 のとき、ビンを以下のように初期化します。
 * <ul>
 * <li>bin 0 [-∞ 〜 0.1)</li>
 * <li>bin 1 [0.1 〜 0.2)</li>
 * <li>bin 2 [0.2 〜 0.3)</li>
 * <li>bin 3 [0.3 〜 0.4)</li>
 * <li>bin 4 [0.4 〜 0.5)</li>
 * <li>bin 5 [0.5 〜 0.6)</li>
 * <li>bin 6 [0.6 〜 0.7)</li>
 * <li>bin 7 [0.7 〜 0.8)</li>
 * <li>bin 8 [0.8 〜 0.9)</li>
 * <li>bin 9 [0.9 〜 +∞)</li>
 * </ul>
 *
 * @constructor lowestUpperBound, highestLowerBound, numBins を指定して処理関数を生成します。
 * @param lowestUpperBound 最小ビンの上限です。
 * @param highestLowerBound 最大ビンの下限です。
 * @param numBins ビン数です。
 * @throws IllegalArgumentException いかのように lowestUpperBound, highestLowerBound, numBins の値が不正なとき
 * numBins == 1 かつそのビンのサイズが無限大でないとき
 * numBins == 2 かつ lowestUpperBound == highestLowerBound でないとき
 * numBins > 2 かつ lowestUpperBound < highestLowerBound でないとき
 */
class HistogramFun(lowestUpperBound: Double = 0.1, highestLowerBound: Double = 0.9, numBins: Int = 10) extends DoubleValueWithIndexFunction with Serializable {
  require(!lowestUpperBound.isNaN, "lowestUpperBound is Double.NaN")
  require(!highestLowerBound.isNaN, "highestLowerBound is Double.NaN")
  require(numBins > 0, s"numBins(=${numBins}) should be >=1")

  /** ビンクラスです。 */
  case class Bin(lowerBound: Double, upperBound: Double, var count: Int) {
    def contains(value: Double) = lowerBound <= value && value < upperBound
  }

  val bins = (numBins match {
    case 1 =>
      require(lowestUpperBound == Double.PositiveInfinity && highestLowerBound == Double.NegativeInfinity, s"if numBins == 1 then lowestUpperBound(=${lowestUpperBound}) should be Double.PositiveInfinity")
      Seq(Double.NegativeInfinity, Double.PositiveInfinity)
    case 2 =>
      require(lowestUpperBound == highestLowerBound, s"if numBins == 2 then lowestUpperBound(=${lowestUpperBound}) should equal highestLowerBound(=${highestLowerBound})")
      Seq(Double.NegativeInfinity, lowestUpperBound, Double.PositiveInfinity)
    case _ =>
      require(highestLowerBound > lowestUpperBound, s"lowestUpperBound(=${lowestUpperBound}) < highestLowerBound(=${highestLowerBound})")
      val rangeWidth = highestLowerBound - lowestUpperBound
      val rangeBins = numBins - 2
      val width = rangeWidth / rangeBins
      Double.NegativeInfinity +: (0 to rangeBins).map(_ * width + lowestUpperBound) :+ Double.PositiveInfinity
  }).sliding(2).map { case Seq(lowerBound, upperBound) => Bin(lowerBound, upperBound, 0) }.toArray

  /**
   * 入力窓内の要素の key 毎に value のヒストグラムを返します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, histogram) key: 集約キー, histogram: ヒストグラムです。histogram.sum = 1.0 となるように正規化されています。
   * @throws IllegalArgumentException rdd == null または rdd の要素に Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity があるとき
   */
  def apply[_Key: ClassTag, _Index: Ordering](rdd: RDD[(_Key, (_Index, Double))]): RDD[(_Key, Seq[Double])] = {
    require(rdd != null, "rdd is null")

    // 分散処理されるときに bins が複製されることを期待。
    rdd.aggregateByKey(bins.clone)(
      (aggregate, valueWithIndex) => {
        val value = checkValue(valueWithIndex)._2;
        aggregate.find(_.contains(value)) match {
          case Some(bin) => bin.count += 1
          case None      => ()
        }
        aggregate
      },
      (left, right) => {
        (left zip right).foreach { case (l, r) => l.count += r.count }
        left
      }
    ).mapValues {
        bins =>
          val count = bins.map(_.count).sum
          bins.map(_.count.toDouble / count)
      }
  }
}

/**
 * 入力窓内の要素を文字列として連結したものを返す関数です。
 *
 * @constructor セパレータを指定して関数を初期化します。
 * @param separator セパレータです。
 * @throws IllegalArgumentException separator == null のとき
 */
class ConcatFun(separator: String = " ") extends StringValueWithIndexFunction with Serializable {
  require(separator != null, "separator is null")

  /**
   * 入力窓内の要素の key 毎に index 順で text を結合します。
   * text の間には separator を結合します。
   * もし、 text.isEmpty のときはその要素をスキップし結合しません。
   * @param rdd 入力窓 要素(key, (index, text)) key: 集約キー, index: 結合順序, text: 対象テキスト。
   * @return 結果 (key, text) key: 集約キー, text 結果テキスト
   * @throws IllegalArgumentException rdd == null のとき
   * @throws NullPointerException text == null のとき。
   */
  def apply[_Key: ClassTag, _Index: Ordering](rdd: RDD[(_Key, (_Index, String))]): RDD[(_Key, String)] = {
    require(rdd != null, "rdd is null")

    rdd.groupByKeyAndSortValues(checkValue).mapValues {
      _.foldLeft(new StringBuilder) {
        case (aggregate, (index, text)) => (aggregate.length, text) match {
          case (_, "")   => aggregate
          case (0, text) => aggregate.append(text)
          case (_, text) => aggregate.append(separator).append(text)
        }
      }.toString()
    }
  }
}

/**
 * 入力窓内の要素のなかで最も高頻度で現れる要素を返す関数です。
 */
object MaxElemFun {
  /**
   * 入力窓内の要素の key 毎に最高頻度の value を返します。
   * 空文字も value の一種類として頻度を計算します。
   * @param rdd 入力窓 要素(key, (index, value)) key: 集約キー, index: 順序, value: 計算対象値です。
   * @return 結果 (key, value) key: 集約キー, value 計算対象値です。もし同頻度の計算対象値が複数あったときは index が最大の計算対象値を返します。
   * @throws IllegalArgumentException rdd == null のとき
   * @throws NullPointerException value == null のとき
   */
  def apply[_Key: ClassTag, _Index: Ordering, _Value: Ordering](rdd: RDD[(_Key, (_Index, _Value))]): RDD[(_Key, _Value)] = {
    require(rdd != null, "rdd is null")

    /** 頻度集計カウンタクラスです。 */
    case class Counter(var maxIndex: _Index, var count: Int) { def +=(operand: Int) = count += operand }

    /** 頻度 → index 順序定義 */
    implicit val counterOrdering: Ordering[Counter] = new Ordering[Counter] {
      import scala.math.Ordered.orderingToOrdered
      override def compare(left: Counter, right: Counter) = left.count - right.count match {
        case 0     => left.maxIndex.compare(right.maxIndex)
        case other => other
      }
    }

    import scala.collection.mutable.HashMap

    def update(aggregate: HashMap[_Value, Counter], index: _Index, value: _Value, operand: Int): Unit = {
      import scala.math.Ordered.orderingToOrdered
      val element = aggregate.getOrElseUpdate(value, Counter(index, 0))
      if (element.maxIndex < index) element.maxIndex = index
      element += operand
    }

    rdd.aggregateByKey(new HashMap[_Value, Counter])(
      {
        case (_, (index, null)) => throw new NullPointerException(s"(${index},null)'s value is null")
        case (aggregate, (index, value)) =>
          update(aggregate, index, value, 1)
          aggregate
      }, (left, right) => {
        right.foreach {
          case (key, Counter(index, count)) =>
            update(left, index, key, count)
        }
        left
      }
    ).mapValues(_.maxBy { case (value, counter) => (counter, value) }._1)
  }

}
