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

import org.apache.spark.AccumulatorParam

/**
 * Remembers the largest item that was added.
 *
 * Taking the maximum is an associative operation, therefore
 * we can implement it as an accumulator.
 */
class MaxOptionAccumulatorParam[U](implicit ord: Ordering[U]) extends AccumulatorParam[Option[U]] {
  override def zero(initialValue: Option[U]): Option[U] = initialValue

  override def addInPlace(r1: Option[U], r2: Option[U]): Option[U] = {
    (r1, r2) match {
      case (Some(a), Some(b)) =>
        Some(ord.max(a, b))
      case (Some(a), None) =>
        r1
      case (None, Some(b)) =>
        r2
      case _ =>
        None
    }
  }
}
