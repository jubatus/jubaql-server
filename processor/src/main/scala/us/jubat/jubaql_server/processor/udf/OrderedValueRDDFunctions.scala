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

import scala.collection.SortedSet
import scala.collection.immutable
import scala.collection.mutable.TreeSet
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkException

/**
 * RDD に Ordering[_Value] で順序付するための関数です。
 */
class OrderedValueRDDFunctions[_Key, _Value](self: RDD[(_Key, _Value)])(implicit kt: ClassTag[_Key], vt: ClassTag[_Value], valueOrdering: Ordering[_Value]) extends Serializable {

  /**
   * Group the values for each key in the RDD into a single sorted sequence.
   *
   * Note: This operation may be very expensive.
   * If you are grouping in order to perform an aggregation (such as a sum or average) over each key,
   * using PairRDDFunctions.aggregateByKey or PairRDDFunctions#reduceByKey will provide much better performance.
   */
  def groupByKeyAndSortValues(filter: _Value => _Value): RDD[(_Key, SortedSet[_Value])] = {
    /**
     * 2つの [[SortedSet]] をマージした [[SortedSet]] です。
     */
    class MergeSortedSet(left: SortedSet[_Value], right: SortedSet[_Value]) extends immutable.SortedSet[_Value] with Serializable with LazyLogging {
      override def size = left.size + right.size
      implicit def ordering = valueOrdering

      private def merge(left: BufferedIterator[_Value], right: BufferedIterator[_Value]): Iterator[_Value] =
        new scala.collection.Iterator[_Value] with Serializable {
          import valueOrdering.mkOrderingOps
          override def hasNext = left.hasNext || right.hasNext
          override def next(): _Value =
            if (left.isEmpty) right.next()
            else if (right.isEmpty) left.next()
            else if (left.head < right.head) left.next()
            else right.next()
        }

      def iterator: Iterator[_Value] = merge(left.iterator.buffered, right.iterator.buffered)

      def -(elem: _Value): immutable.SortedSet[_Value] = throw new UnsupportedOperationException
      def +(elem: _Value): immutable.SortedSet[_Value] = throw new UnsupportedOperationException
      def contains(elem: _Value): Boolean = throw new UnsupportedOperationException
      def rangeImpl(from: Option[_Value], until: Option[_Value]): immutable.SortedSet[_Value] = throw new UnsupportedOperationException
    }

    self.combineByKey(
      (value) => (new TreeSet[_Value]() += filter(value)),
      (collect, value) => collect.asInstanceOf[TreeSet[_Value]] += filter(value),
      (left, right) => new MergeSortedSet(left, right)
    )
  }
}

/**
 * implicit conversion 用
 */
object OrderedValueRDDFunctions {
  implicit def rddToOrderedValueRDDFunctions[_Key, _Value](rdd: RDD[(_Key, _Value)])(implicit kt: ClassTag[_Key], vt: ClassTag[_Value], valueOrdering: Ordering[_Value] = null) = new OrderedValueRDDFunctions(rdd)
}
