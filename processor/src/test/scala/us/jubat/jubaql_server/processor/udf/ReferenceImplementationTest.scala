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

import org.scalatest.Matchers
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.FlatSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Gen

class ReferenceImplementationTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks with LazyLogging {
  "ReferenceImplementation shuffle and sort" should "return 同じ順序の values" in {
    val values = (1 to 100).map((_, 1d))
    ReferenceImplementation.sortByIndex(ReferenceImplementation.shuffle(values)) should be(values)
  }

  "ReferenceImplementation.stripIndex" should "return インデックスを除いた values" in {
    ReferenceImplementation.stripIndex(Seq((0, 1d), (1, 1d), (2, 1d))) should be(Seq(1d, 1d, 1d))
  }

  "ReferenceImplementation.average" should "be correct" in {
    ReferenceImplementation.average(Seq(1d, 2d, 3d)) should be(2d)
  }

  "ReferenceImplementation.standardDeviation" should "be correct" in {
    ReferenceImplementation.standardDeviation(Seq(1d, 2d, 3d)) should be(0.816496580927726 +- 1E-15)
    ReferenceImplementation.standardDeviation(Seq(4.72398231760014E-249, -5.494663445551323E-162)) should be(2.2227587494850775E-162 +- 1E-15)
  }

  "ReferenceImplementation.quantile" should "be correct" in {
    ReferenceImplementation.quantile(Seq(2d, 1d), 0.5d) should be(2d)
    ReferenceImplementation.quantile(Seq(1d, 2d), 0.5d) should be(2d)

    ReferenceImplementation.quantile(Seq(1d, 2d, 3d), 0.0d) should be(1d)
    ReferenceImplementation.quantile(Seq(1d, 2d, 3d), 0.5d) should be(2d)
    ReferenceImplementation.quantile(Seq(1d, 2d, 3d), 1.0d) should be(3d)
    ReferenceImplementation.quantile(Seq(2.306041230747762E-295, 8.27061849935151E112, 1.7803154582292577E179), 0.5) should be(8.27061849935151E112)
  }

  "ReferenceImplementation.regressionline" should "be correct" in {
    ReferenceImplementation.regressionline(Seq(0d, 1d, 2d)) should be((1d, 0d))
  }

  "ReferenceImplementation.fourierCoefficients" should "be correct" in {
    import math._
    val N = 128
    val values = (0 until N).map(t => sin(2 * Pi * t / N) + cos(2 * Pi * t / N)).toArray

    import math._

    /**
     * @see http://www.riken.jp/brict/Ijiri/study/FourierTransform_disc.html
     */
    def fourieTrans1D(fl: Array[Double]) = {
      val N = fl.length
      val w0 = 2 * Pi / N

      val Rk = Array.ofDim[Double](N)
      val Ik = Array.ofDim[Double](N)

      for (k <- 0 until N) {
        Rk(k) = 0
        Ik(k) = 0
        for (m <- 0 until N) {
          Rk(k) += fl(m) * cos(w0 * k * m)
          Ik(k) += -fl(m) * sin(w0 * k * m)
        }
        Rk(k) /= N
        Ik(k) /= N
      }

      (Rk, Ik)
    }

    def validLength = for {
      size <- Gen.choose(1, 8)
      values <- Gen.listOfN(pow(2, size).toInt, Gen.choose(-100.0, +100.0))
    } yield values

    forAll(validLength) { (values) =>
      val naive = fourieTrans1D(values.toArray)
      val target = ReferenceImplementation.fourierCoefficients(values)

      def shouldBeEqualsAbsValues(v: (Double, Double)): Unit = v match { case (l, r) => abs(l) should be(abs(r) +- 1E-10) }
      target._1.zip(naive._1).foreach(shouldBeEqualsAbsValues)
      target._2.zip(naive._2).foreach(shouldBeEqualsAbsValues)
    }
  }

  "ReferenceImplementation.waveletCoefficients" should "be correct" in {
    ReferenceImplementation.waveletCoefficients(Seq[Double]()) should be(Seq[Double]())
    ReferenceImplementation.waveletCoefficients(Seq(0d)) should be(Seq(0d))
    ReferenceImplementation.waveletCoefficients(Seq(32d, 10d, 20d, 38d, 37d, 28d, 38d, 34d, 18d, 24d, 18d, 9d, 23d, 24d, 28d, 34d)) should be(Seq(25.9375d, 3.6875d, -4.625d, -5d, -4d, -1.75d, 3.75d, -3.75d, 11d, -9d, 4.5d, 2d, -3d, 4.5d, -0.5d, -3d))
    ReferenceImplementation.waveletCoefficients(Seq(3d, 1d, 0d, 4d, 8d, 6d, 9d, 9d)) should be(Seq(5d, -3d, 0d, -1d, 1d, -2d, 1d, 0d))
    ReferenceImplementation.waveletCoefficients(Seq(9d, 7d, 3d, 5d, 6d, 10d, 2d, 6d)) should be(Seq(6d, 0d, 2d, 2d, 1d, -1d, -2d, -2d))
  }

  "ReferenceImplementation.histogram" should "be correct" in {
    ReferenceImplementation.histogram(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 1, 9, 10) should be(Seq(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1))
    ReferenceImplementation.histogram(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 0, 10, 7) should be(Seq(0.0, 0.2, 0.2, 0.2, 0.2, 0.2, 0.0))
    ReferenceImplementation.histogram(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 1, 9, 1) should be(Seq(1.0))
    ReferenceImplementation.histogram(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 1, 1, 2) should be(Seq(0.1, 0.9))
    ReferenceImplementation.histogram(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 1, 9, 3) should be(Seq(0.1, 0.8, 0.1))
  }

  "ReferenceImplementation.concatenate" should "be correct" in {
    an[IllegalArgumentException] should be thrownBy ReferenceImplementation.concatenate(Seq[String](), null)
    an[IllegalArgumentException] should be thrownBy ReferenceImplementation.concatenate(Seq("a", "b"), null)
    an[NullPointerException] should be thrownBy ReferenceImplementation.concatenate(Seq[String](null), ",")
    ReferenceImplementation.concatenate(Seq[String](), ",") should be("")
    ReferenceImplementation.concatenate(Seq("a"), ",") should be("a")
    ReferenceImplementation.concatenate(Seq("a", "b"), ",") should be("a,b")
    ReferenceImplementation.concatenate(Seq("a", "b", "c"), ",") should be("a,b,c")

    forAll { (values: Seq[String], separator: String) =>
      ReferenceImplementation.concatenate(values, separator) should be(values.filterNot(_.isEmpty).mkString(separator))
    }
  }

  "ReferenceImplementation.mode" should "be correct" in {
    an[IllegalArgumentException] should be thrownBy ReferenceImplementation.mode(Seq[String]())
    ReferenceImplementation.mode(Seq("a")) should be("a")
    ReferenceImplementation.mode(Seq("a", "b")) should be("b")
    ReferenceImplementation.mode(Seq("a", "b", "a")) should be("a")
    ReferenceImplementation.mode(Seq("a", "b", "a", "b")) should be("b")
    ReferenceImplementation.mode(Seq("f", "m", "f", "m")) should be("m")

    an[NullPointerException] should be thrownBy ReferenceImplementation.mode(Seq("a", null))
  }
}