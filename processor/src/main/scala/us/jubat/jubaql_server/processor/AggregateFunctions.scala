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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.types._

import scala.reflect.ClassTag

sealed trait SomeAggregateFunction[IN] {
  def aggFun(rdd: RDD[(Long, (Long, IN))]): RDD[(Long, Any)]

  val inType: DataType
  val outType: DataType

  implicit protected def toLongAnyRDD[T: ClassTag](rdd: RDD[(Long, T)]) =
    rdd.mapValues(_.asInstanceOf[Any])
}

trait DoubleInputAggFun extends SomeAggregateFunction[Double] {
  override val inType: DataType = DoubleType
}

object AvgFun extends DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    us.jubat.jubaql_server.processor.udf.AvgFun.apply(rdd)

  override val outType: DataType = DoubleType
}

object StdDevFun extends DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    us.jubat.jubaql_server.processor.udf.StdDevFun.apply(rdd)

  override val outType: DataType = DoubleType
}

class QuantileFun(position: Double = 0.5d)
  extends us.jubat.jubaql_server.processor.udf.QuantileFun(position)
  with DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    apply(rdd)

  override val outType: DataType = DoubleType
}

object LinApproxFun extends DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    us.jubat.jubaql_server.processor.udf.LinApproxFun.apply(rdd).mapValues(ab => {
      Map("a" -> ab._1, "b" -> ab._2).asInstanceOf[Any]
    })

  override val outType: DataType =
    StructType(StructField("a", DoubleType, nullable = true) ::
      StructField("b", DoubleType, nullable = true) :: Nil)
}

object FourierCoeffsFun extends DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    us.jubat.jubaql_server.processor.udf.FourierCoeffsFun.apply(rdd).mapValues(reIm => {
      Map("re" -> reIm._1, "im" -> reIm._2).asInstanceOf[Any]
    })

  override val outType: DataType =
    StructType(StructField("re", ArrayType(DoubleType, containsNull = false), nullable = true) ::
      StructField("im", ArrayType(DoubleType, containsNull = false), nullable = true) :: Nil)
}

object WaveletCoeffsFun extends DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    us.jubat.jubaql_server.processor.udf.WaveletCoeffsFun.apply(rdd)

  override val outType: DataType = ArrayType(DoubleType, containsNull = false)
}

class HistogramFun(lowestUpperBound: Double = 0.1,
                   highestLowerBound: Double = 0.9,
                   numBins: Int = 10)
  extends us.jubat.jubaql_server.processor.udf.HistogramFun(lowestUpperBound, highestLowerBound, numBins)
  with DoubleInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, Double))]) =
    apply(rdd)

  override val outType: DataType = ArrayType(DoubleType, containsNull = false)
}

trait StringInputAggFun extends SomeAggregateFunction[String] {
  override val inType: DataType = StringType
}

class ConcatFun(separator: String = " ")
  extends us.jubat.jubaql_server.processor.udf.ConcatFun(separator)
  with StringInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, String))]) =
    apply(rdd)

  override val outType: DataType = StringType
}

object MaxElemFun extends StringInputAggFun {
  override def aggFun(rdd: RDD[(Long, (Long, String))]) =
  // for some reason in (only) this case, type parameters
  // to apply() must be specified explicitly
    us.jubat.jubaql_server.processor.udf.MaxElemFun.apply[Long, Long, String](rdd)

  override val outType: DataType = StringType
}

object AggregateFunctions {
  type AggFunOrError = Either[String, SomeAggregateFunction[_]]

  // check parameter types for aggregate functions

  def checkAvgParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("avg", params, AvgFun).check
  }

  def checkStdDevParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("stddev", params, StdDevFun).check
  }

  def checkQuantileParams(params: List[Expression]): AggFunOrError = {
    params match {
      // version with provided p parameter
      case pExp :: exp :: Nil =>
        if (!pExp.foldable) {
          Left("first parameter to quantile must be evaluable")
        } else {
          if (!pExp.dataType.isInstanceOf[NumericType]) {
            Left(s"wrong type of parameters for quantile (must be (numeric, numeric))")
          } else {
            val pBox = pExp.eval()
            pBox match {
              case p: Double if p >= 0 && p <= 1.0 =>
                Right(new QuantileFun(p))
              case _ =>
                Left("first parameter to quantile must be in [0,1] range")
            }
          }
        }

      // no parameter version
      case others =>
        SingleParamAggFunctionChecker("quantile", others, new QuantileFun()).check
    }
  }

  def checkLinApproxParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("linapprox", params, LinApproxFun).check
  }

  def checkFourierParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("fourier", params, FourierCoeffsFun).check
  }

  def checkWaveletParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("wavelet", params, WaveletCoeffsFun).check
  }

  def checkHistogramParams(params: List[Expression]): AggFunOrError = {
    params match {
      // version with 3 provided parameters (bounds and number of bins)
      case lubExp :: hlbExp :: binExp :: exp :: Nil =>
        if (!lubExp.foldable || !hlbExp.foldable || !binExp.foldable) {
          Left("parameters for histogram must be evaluable")
        } else {
          if (!lubExp.dataType.isInstanceOf[NumericType] ||
            !hlbExp.dataType.isInstanceOf[NumericType] ||
            !binExp.dataType.isInstanceOf[IntegralType]) {
            Left(s"wrong type of parameters for histogram (must be (numeric, numeric, integer, numeric))")
          } else {
            (lubExp.eval(), hlbExp.eval(), binExp.eval()) match {
              case (lub: Double, hlb: Double, bin: Int) =>
                Right(new HistogramFun(lub, hlb, bin))
              case _ =>
                Left("wrong type of parameters for histogram, must be (double, double, int, numeric)")
            }
          }
        }

      // version with 2 provided parameters (bounds)
      case lubExp :: hlbExp :: exp :: Nil =>
        if (!lubExp.foldable || !hlbExp.foldable) {
          Left("parameters for histogram must be evaluable")
        } else {
          if (!lubExp.dataType.isInstanceOf[NumericType] ||
            !hlbExp.dataType.isInstanceOf[NumericType]) {
            Left(s"wrong type of parameters for histogram (must be (numeric, numeric, numeric))")
          } else {
            (lubExp.eval(), hlbExp.eval()) match {
              case (lub: Double, hlb: Double) =>
                Right(new HistogramFun(lub, hlb))
              case _ =>
                Left("wrong type of parameters for histogram, must be (double, double, numeric)")
            }
          }
        }

      // version with 1 provided parameter (bins)
      case binExp :: exp :: Nil =>
        if (!binExp.foldable) {
          Left("parameters for histogram must be evaluable")
        } else {
          if (!binExp.dataType.isInstanceOf[IntegralType]) {
            Left(s"wrong type of parameters for histogram (must be (integer, numeric))")
          } else {
            (binExp.eval()) match {
              case bin: Int =>
                Right(new HistogramFun(numBins = bin))
              case _ =>
                Left("wrong type of parameters for histogram, must be (int, numeric)")
            }
          }
        }

      // no parameter version
      case others =>
        SingleParamAggFunctionChecker("histogram", others, new HistogramFun()).check
    }
  }

  def checkConcatParams(params: List[Expression]): AggFunOrError = {
    params match {
      // version with provided p parameter
      case cExp :: exp :: Nil =>
        if (!cExp.foldable) {
          Left("first parameter to concat must be evaluable")
        } else {
          if (!cExp.dataType.equals(StringType)) {
            Left(s"wrong type of parameters for concat (must be (string, string))")
          } else {
            val cBox = cExp.eval()
            cBox match {
              case c: String =>
                Right(new ConcatFun(c))
              case _ =>
                Left("first parameter to concat must be a string")
            }
          }
        }

      // no parameter version
      case others =>
        SingleParamAggFunctionChecker("concat", others, new ConcatFun()).check
    }
  }

  def checkMaxElemParams(params: List[Expression]): AggFunOrError = {
    SingleParamAggFunctionChecker("maxelem", params, MaxElemFun).check
  }

  private case class SingleParamAggFunctionChecker[T <:
  SomeAggregateFunction[_]](name: String,
                            params: List[Expression],
                            obj: T) {
    def check: Either[String, T] = {
      params match {
        case exp :: Nil =>
          Right(obj)
        case _ =>
          Left(s"wrong number of parameters for $name (must be 1)")
      }
    }
  }

}
