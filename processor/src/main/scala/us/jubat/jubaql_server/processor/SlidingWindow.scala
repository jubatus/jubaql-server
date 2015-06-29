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

import SlidingWindow.ItemKey
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object SlidingWindow {
  /**
   * Compute a timestamp-based sliding window stream from the given stream.
   *
   * This means that the key in the (Long, T) stream passed in as a parameter
   * is interpreted as a timestamp. The items in the stream will be grouped
   * in buckets of length `windowLengthSec` seconds that start with an offset
   * of `slidingIntervalSec` seconds to the previous window. Note that this
   * implies the possibility of varying number of items per window. Empty
   * windows will not be included in the output stream.
   *
   * The stream that is returned consists of items with the form
   * (windowTimestamp, (itemTimestamp, data)) where
   * - windowTimestamp is a stream-unique identifier for the window,
   * - itemTimestamp is the timestamp from the input stream,
   * - windowTimestamp increases by `slidingIntervalSec * 1000` from one
   *   window to the next,
   * - all items that belong to the same window are guaranteed to be in the
   *   same interval/RDD of the window stream,
   * - all items that belong to this window have item timestamps in the range
   *   [windowTimestamp, windowTimestamp + windowLengthSec * 1000).
   *
   * This form of representation was chosen because
   * - although (windowTimestamp, [data1, data2, data3, ...]) is a more natural
   *   and easier to use form, DStream.groupByKey() (which results in such a
   *   form) performs very bad, so that we wanted to allow the use of
   *   reduceByKey(), aggregateByKey() etc. instead,
   * - for algorithms that depend on the order of items (such as concatenation),
   *   we kept the original timestamp in there so that the window-internal
   *   ordering can be restored if required.
   *
   * For example, with windowLengthSec = 3 and slidingIntervalSec = 2, if the
   * input stream looks like
   *  key:  1420788214031 | 1420788214035 | 1420788215132 | ...
   *  val:              a |             b |             c | ...
   * then the output will have the form:
   *  key:  1420788213000      | 1420788213000      | 1420788213000      | 1420788215000      ...
   *  val:  (1420788214031, a) | (1420788214035, b) | (1420788215132, c) | (1420788215132, c) ...
   */
  def byTimestamp[T](stream: DStream[(ItemKey, T)],
                     windowLengthSec: Int,
                     slidingIntervalSec: Int): DStream[(WindowKey, (ItemKey, T))] = {
    // get the count of items in the stream
    val slidingItemCount = slidingTotalItemCount(stream)
    // call the general purpose function
    slidingWindowByKey(stream, slidingItemCount,
      windowLengthSec * 1000, slidingIntervalSec * 1000, countWindowsSequentially = false)
  }

  /**
   * Compute a count-based sliding window stream from the given stream.
   *
   * The items in the stream will be grouped in buckets with `windowLength`
   * items that start with an offset of `slidingInterval` items to the
   * previous window.
   *
   * The stream that is returned consists of items with the form
   * (windowIndex, (itemIndex, data)) where
   * - windowIndex is a stream-unique identifier for the window,
   * - windowIndex increases by 1 from one window to the next,
   * - itemIndex is a stream-unique identifier for the data item,
   * - itemIndex increases by 1 from one item to the next within one window,
   * - all `windowLength` items that belong to the same window are guaranteed
   *   to be in the same interval/RDD of the window stream.
   *
   * This form of representation was chosen because
   * - although (windowIndex, [data1, data2, data3, ...]) is a more natural
   *   and easier to use form, DStream.groupByKey() (which results in such a
   *   form) performs very bad, so that we wanted to allow the use of
   *   reduceByKey(), aggregateByKey() etc. instead,
   * - for algorithms that depend on the order of items (such as concatenation),
   *   we kept the original timestamp in there so that the window-internal
   *   ordering can be restored if required.
   *
   * For example, with windowLength = 3 and slidingInterval = 2, if the input
   * stream looks like
   *  val:  a | b | c | d | e | f | g | h | i  |  j | k | l | m | n  |  o
   * then the output will have the form:
   *  key;  0      | 0      | 0      | 1      | 1      | 1      | 2      | ...
   *  val:  (0, a) | (1, b) | (2, c) | (2, c) | (3, d) | (4, e) | (4, e) | ...
   */
  def byCount[T](stream: DStream[T],
                 windowLength: Int,
                 slidingInterval: Int): DStream[(WindowKey, (ItemKey, T))] = {
    // get the count of items in the stream
    val slidingItemCount = slidingTotalItemCount(stream)
    // get a global index for the items in the stream
    val indexedInputStream = addGlobalIndex(stream, slidingItemCount)
    // call the general purpose function
    slidingWindowByKey(indexedInputStream, slidingItemCount,
      windowLength, slidingInterval, countWindowsSequentially = true)
  }

  type ItemKey = Long
  type WindowKey = Long
  protected type LocalWindowKey = Long
  protected type SubstreamIdx = Int

  /**
   * Compute a key-based sliding window stream from the given stream.
   *
   * The items in the stream will be grouped in buckets so that each bucket
   * contains all items with keys in the range `[x, x + windowLength)`, where
   * x increases by `slidingInterval` from one bucket to the next. Note how
   * count-based and timestamp-based windows are a special case of this (with
   * index or timestamp as key, respectively). Also note that this implies
   * the possibility of varying number of items per window. Empty windows will
   * not be included in the output stream.
   *
   * The stream that is returned consists of items with the form
   * (windowKey, (itemKey, data)) where
   * - windowKey is a stream-unique identifier for the window,
   * - itemKey is the key from the input stream.
   * - all items that belong to the same window are guaranteed to be in the
   *   same interval/RDD of the window stream.
   *
   * This form of representation was chosen because
   * - although (windowKey, [data1, data2, data3, ...]) is a more natural
   *   and easier to use form, DStream.groupByKey() (which results in such a
   *   form) performs very bad, so that we wanted to allow the use of
   *   reduceByKey(), aggregateByKey() etc. instead,
   * - for algorithms that want to use the original key (e.g., to infer
   *   order), we keep the original key in the output stream, too.
   *
   * The shape of the windowKey can be controlled via the parameter
   * `countWindowsSequentially`:
   * - If this value is true, then windowKey will start at 0 and increase by
   *   1 between windows.
   * - If this value is false, then windowKey will be the `x` from the
   *   introduction paragraph, i.e., correspond to the key values contained
   *   in the window and increase by `slidingInterval`.
   *
   * `slidingItemCount` is a stream as computed by `slidingTotalItemCount()`
   * on the input stream.
   */
  protected def slidingWindowByKey[T](stream: DStream[(ItemKey, T)],
    slidingItemCount: DStream[ItemCountPair],
    windowLength: Int, slidingInterval: Int,
    countWindowsSequentially: Boolean): DStream[(WindowKey, (ItemKey, T))] = {

    type KeyedItem = (ItemKey, T)
    // stream is expected to look like:
    //  key:  0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8  ||  9 | 10 | 11 | 12 | 13  ||  14
    //  val:  a | b | c | d | e | f | g | h | i  ||  j |  k |  l |  m |  n  ||   o
    // or
    //  key:  1420788214031 | 1420788214035 | 1420788214132 ||  ...
    //  val:              a |             b |             c ||  ...

    // slidingItemCount is expected to look like:
    //  val:  ItemCountPair(0,9)                  ||  ItemCountPair(9,14)   ||  ...
    // or
    //  val:  ItemCountPair(0,3)                            ||  ...

    /*
     * First, read this overview of the algorithm. The documentation for the
     * single steps are repeated again at the correct position.
     *
     *************************************************************************
     *
     * Our algorithm consists of the following steps:
     * 1) Compute how many substreams (streams responsible for a certain subset
     *    of the windows that can be computed without overlaps, identified by
     *    the offset of their first window) we will need.
     * 2) For every substream and every interval of the input stream:
     *    2.1) Filter out all items that do not belong to any window of that
     *         substream.
     *         => `someWindowItems`
     *    2.2) Compute a substream-local window key for each item.
     *         => `itemsByLocalWindowKey`
     *    2.3) Compute the largest window key.
     *         => `maxLocalWindowKey`
     * 3) Update a state stream that holds the largest window key per
     *    substream for this interval and the previous interval.
     *    => `maxLocalWindowKeyPerSubstream`
     * 4) For every substream and every interval of the input stream:
     *    4.1) Get back `itemsByLocalWindowKey` as computed in the previous loop.
     *    4.2) Get the largest window key in this substream for this and
     *         the previous interval from the state stream.
     *         => `maxLocalWindowKey`
     *    4.3) Decide how to process each item depending on the substream-local
     *         window key and add a `MergeInfo` flag:
     *         a) Window key equals the largest window key in the previous
     *            interval:
     *            => This item belongs to a partial window that we already saw
     *               in the last interval. We assume that the previously seen
     *               items of this partial window are stored in the state
     *               stream. We will merge this item with the state stream.
     *               => MergeInfo.PREV
     *         b) Window key equals the largest window key in this interval:
     *            => There might be data in future intervals that actually
     *               belongs to the same window. We will keep this item in the
     *               state stream for later processing.
     *               => MergeInfo.NEXT
     *         c) Otherwise:
     *            => We can be sure that all items of this item's window are
     *               contained in this interval, so we will process them right
     *               now.
     *               => MergeInfo.NOW
     *         => `itemsWithMergeInfo`
     *    4.4) Transform the substream-local window key into a window key
     *         that is unique across all substreams.
     *         => `itemsByGlobalWindowKey`
     *    4.5) Now, pick all items that are to be processed now, i.e. in windows
     *         where there is no doubt all items are in the current interval.
     *         => `completeWindowItems`
     *    4.6) For the rest of the items, a state update is necessary. We want
     *         to add an indicator to the state update function if the set of
     *         "items in the state" plus "items now merged with the state"
     *         forms a complete window, i.e., if these items can also be
     *         processed now. If the number of all items seen in the interval
     *         (belonging to a window or not) is larger than the number of items
     *         with the MergeInfo.PREV property, we know that there can be no
     *         further items in future intervals that belong to the same partial
     *         window, so we add a dummy item with a MergeInfo.FIN marker to
     *         indicate to the state stream update that this partial window can
     *         be considered complete.
     *         (There are only two situations where this is not the case:
     *         Either the interval was empty or all items were PREV items.
     *         In both situations there may be more data for that window
     *         in future intervals.)
     *         => `partialWindowItems`
     * 5) Update a state stream that holds all items in partial windows per
     *    substream. (Remember that every substream works with overlap-free
     *    windows, therefore there can only be one partial window per
     *    substream waiting for future data.) If there are items with
     *    MergeInfo.NEXT or MergeInfo.FIN flag in the update data, flag
     *    the previous data as "can be processed".
     * 6) Union the "can be processed" items in the state stream and the
     *    "complete windows" from each substream to obtain the final result.
     *
     *************************************************************************
     */

    /*
     * 1) Compute how many substreams (streams responsible for a certain subset
     *    of the windows that can be computed without overlaps, identified by
     *    the offset of their first window) we will need.
     */

    // numberOfStreams depends on how much the windows overlaps
    val numberOfStreams = (windowLength - 1) / slidingInterval + 1

    // batchSize is the length of a window *plus* the length of skipped elements
    // until the next window in the same stream starts
    val batchSize = numberOfStreams * slidingInterval

    // stores the highest local window key in this interval for each substream
    val maxLocalWindowKeysPerSubstream: mutable.Map[SubstreamIdx,
      DStream[(SubstreamIdx, LocalWindowKey)]] = mutable.Map()

    // constant that holds a value smaller than any valid window index to detect
    // that there have been no windows yet
    val impossiblySmallWindowKey = -1L

    // stores all items that are in some (partial or complete) window,
    // keyed by local window key, for each substream
    val allWindowItemsPerSubstream: mutable.Map[SubstreamIdx,
      DStream[(WindowKey, KeyedItem)]] = mutable.Map()

    // stores all items that are in some partial window for each substream
    val partialWindowsPerSubstream: mutable.Map[SubstreamIdx,
      DStream[(SubstreamIdx, (WindowKey, (MergeInfo.Value, KeyedItem)))]] =
      mutable.Map()

    /*
     * 2) For every substream and every interval of the input stream:
     */

    // a for loop leads to a NotSerializableException, so fall back to while
    var _i = 0
    while (_i < numberOfStreams) {
      // We need an additional `currentSubstream` val or we will close over the
      // var object instead of the current value
      val currentSubstream = _i

      // The currentStreamOffset is the lower bound for the items in the first
      // window that will be processed in the current substream:
      val currentStreamOffset = currentSubstream * slidingInterval

      /*
       * 2.1) Filter out all items that do not belong to any window of that
       *      substream.
       */

      val someWindowItems: DStream[KeyedItem] = stream.filter(kv => {
        val itemKey = kv._1
        (itemKey + batchSize - currentStreamOffset) % batchSize < windowLength &&
          itemKey >= currentStreamOffset
      })

      /*
       * 2.2) Compute a substream-local window key for each item.
       */

      val itemsByLocalWindowKey: DStream[(LocalWindowKey, KeyedItem)] =
        someWindowItems.map(kv => {
          val itemKey = kv._1
          val localWindowKey = (itemKey - currentStreamOffset) / batchSize
          (localWindowKey, kv)
        })
      allWindowItemsPerSubstream += ((currentSubstream, itemsByLocalWindowKey))

      /*
       * 2.3) Compute the largest window key.
       */

      // the following code computes a 1-element RDD with the maximal
      // local window key in this substream; the code works similar to
      // DStream.count
      val zeroValue = itemsByLocalWindowKey.context.sparkContext.makeRDD(Seq((currentSubstream,
        impossiblySmallWindowKey)))
      val maxLocalWindowKey: DStream[(SubstreamIdx, ItemKey)] = itemsByLocalWindowKey
        .map(x => (currentSubstream, x._1))
        .transform(_.union(zeroValue))
        .reduceByKey((a, b) => Math.max(a, b))
      maxLocalWindowKey.cache()

      maxLocalWindowKeysPerSubstream += ((currentSubstream, maxLocalWindowKey))

      _i += 1
    }

    /*
     * 3) Update a state stream that holds the largest window key per
     *    substream for this interval and the previous interval.
     */

    val updateMaxWindowKeyState: (Seq[ItemKey], Option[MaxKeyPair]) => Option[MaxKeyPair] =
      (itemCounts, maybePreviousState) => {
        val previousState = maybePreviousState.getOrElse(MaxKeyPair(impossiblySmallWindowKey,
          impossiblySmallWindowKey))
        val newMaxId = itemCounts.head
        if (newMaxId == impossiblySmallWindowKey) {
          // meaning: there was no window in this interval
          Some(previousState)
        } else {
          val newState = previousState.shiftTo(newMaxId)
          Some(newState)
        }
      }
    val maxLocalWindowKeyPerSubstream = maxLocalWindowKeysPerSubstream.values
      .reduceLeft(_.union(_))
      .updateStateByKey(updateMaxWindowKeyState)
    maxLocalWindowKeyPerSubstream.cache()

    // stores all items that are in a complete window, keyed by local
    // window key, for each substream
    val completeWindowsPerSubstream: mutable.Map[SubstreamIdx,
      DStream[(WindowKey, KeyedItem)]] = mutable.Map()

    /*
     4) For every substream and every interval of the input stream:
     */

    // a for loop leads to a NotSerializableException, so fall back to while
    _i = 0
    while (_i < numberOfStreams) {
      // We need an additional `currentSubstream` val or we will close over the
      // var object instead of the current value
      val currentSubstream = _i

      /*
       * 4.1) Get back `itemsByLocalWindowKey` as computed in the previous loop.
       */

      // restore the itemsByLocalWindowKey from the map
      val itemsByLocalWindowKey = allWindowItemsPerSubstream(currentSubstream)

      /*
       * 4.2) Get the largest window key in this substream for this and
       *      the previous interval from the state stream.
       */

      // get this substream's state from the global state stream
      val maxLocalWindowKey = maxLocalWindowKeyPerSubstream
        .filter(_._1 == currentSubstream).map(_._2)

      /*
       * 4.3) Decide how to process each item depending on the substream-local
       *      window key and add a `MergeInfo` flag:
       */

      val addMergeInfo: (RDD[(LocalWindowKey, KeyedItem)], RDD[MaxKeyPair]) =>
        RDD[(LocalWindowKey, (MergeInfo.Value, KeyedItem))] = (items, maxKeyPairs) => {
        val maxWindowKeyPair = maxKeyPairs.take(1).head
        items.map(keyVal => {
          val (localWindowKey, data) = keyVal

          /*
           * a) Window key equals the largest window key in the previous
           *    interval:
           *    => This item belongs to a partial window that we already saw
           *       in the last interval. We assume that the previously seen
           *       items of this partial window are stored in the state
           *       stream. We will merge this item with the state stream.
           */

          if (localWindowKey <= maxWindowKeyPair.previousMaxKey) {
            (localWindowKey, (MergeInfo.PREV, data))
          }

          /*
           * b) Window key equals the largest window key in this interval:
           *    => There might be data in future intervals that actually
           *       belongs to the same window. We will keep this item in the
           *       state stream for later processing.
           */

          else if (localWindowKey == maxWindowKeyPair.currentMaxKey) {
            // NB. In this setup, the last window in this interval
            // will always marked as partial and therefore go
            // to the state. This could be optimized.
            (localWindowKey, (MergeInfo.NEXT, data))
          }

          /*
           * c) Otherwise:
           *    => We can be sure that all items of this item's window are
           *       contained in this interval, so we will process them right
           *       now.
           */

          else {
            (localWindowKey, (MergeInfo.NOW, data))
          }
        })
      }
      val itemsWithMergeInfo: DStream[(LocalWindowKey, (MergeInfo.Value, KeyedItem))] =
        itemsByLocalWindowKey.transformWith(maxLocalWindowKey, addMergeInfo)

      /*
       * 4.4) Transform the substream-local window key into a window key
       *      that is unique across all substreams.
       */

      // either count windows starting from 0 and increment by 1,
      // or assign a key corresponding to the lowest (rounded)
      // key in the window
      val transformLocalToGlobalKey: LocalWindowKey => WindowKey =
        if (countWindowsSequentially)
          _ * numberOfStreams + currentSubstream
        else
          _ * batchSize + currentSubstream * slidingInterval
      val itemsByGlobalWindowKey: DStream[(WindowKey, (MergeInfo.Value, KeyedItem))] =
        itemsWithMergeInfo.map(keyVal => {
          val (localWindowKey, itemWithMergeInfo) = keyVal
          val globalWindowKey = transformLocalToGlobalKey(localWindowKey)
          (globalWindowKey, itemWithMergeInfo)
        })

      /*
       * 4.5) Now, pick all items that are to be processed now, i.e. in windows
       *      where there is no doubt all items are in the current interval.
       */

      val completeWindows = itemsByGlobalWindowKey
        .filter(_._2._1 == MergeInfo.NOW)
        .map(kv => (kv._1, kv._2._2))
      completeWindowsPerSubstream += ((currentSubstream, completeWindows))

      /*
       * 4.6) For the rest of the items, a state update is necessary. We want
       *      to add an indicator to the state update function if the set of
       *      "items in the state" plus "items now merged with the state"
       *      forms a complete window, i.e., if these items can also be
       *      processed now. If the number of all items seen in the interval
       *      (belonging to a window or not) is larger than the number of items
       *      with the MergeInfo.PREV property, we know that there can be no
       *      further items in future intervals that belong to the same partial
       *      window, so we add a dummy item with a MergeInfo.FIN marker to
       *      indicate to the state stream update that this partial window can
       *      be considered complete.
       *      (There are only two situations where this is not the case:
       *      Either the interval was empty or all items were PREV items.
       *      In both situations there may be more data for that window
       *      in future intervals.)
       */

      val invalidWindowKey = -1L
      val stateCompleteMarker = itemsByGlobalWindowKey.context.sparkContext.makeRDD(Seq((
        invalidWindowKey, (MergeInfo.FIN, null.asInstanceOf[KeyedItem]))), 1)
      val addStateCompleteMarker: (RDD[(WindowKey, (MergeInfo.Value, KeyedItem))],
        RDD[ItemCountPair]) => RDD[(WindowKey, (MergeInfo.Value, KeyedItem))] =
        (rdd, counts) => {
          val numPrevs = rdd.filter(_._2._1 == MergeInfo.PREV).count
          val countPair = counts.take(1).head
          val countInThisInterval = countPair.currentCount - countPair.previousCount
          if (numPrevs < countInThisInterval) {
            rdd.union(stateCompleteMarker)
          } else {
            rdd
          }
        }
      val partialWindowsItems = itemsByGlobalWindowKey.filter(_._2._1 != MergeInfo.NOW)
      partialWindowsItems.cache()
      val partialWindows: DStream[(SubstreamIdx, (WindowKey, (MergeInfo.Value, KeyedItem)))] =
        partialWindowsItems
          .transformWith(slidingItemCount, addStateCompleteMarker).map((currentSubstream, _))

      partialWindowsPerSubstream += ((currentSubstream, partialWindows))

      _i += 1
    }

    // union the windows computed by each substream
    val allPartialWindows = partialWindowsPerSubstream.values.reduceLeft(_.union(_))
    val allCompleteWindows = completeWindowsPerSubstream.values.reduceLeft(_.union(_))

    /*
     * 5) Update a state stream that holds all items in partial windows per
     *    substream. (Remember that every substream works with overlap-free
     *    windows, therefore there can only be one partial window per
     *    substream waiting for future data.) If there are items with
     *    MergeInfo.NEXT or MergeInfo.FIN flag in the update data, flag
     *    the previous data as "can be processed".
     */

    type STATE = List[(WindowKey, KeyedItem, Boolean)]

    val stateUpdateFunction: (Seq[(WindowKey, (MergeInfo.Value, KeyedItem))],
      Option[STATE]) => Option[STATE] = (newValuesInInterval, maybePreviousState) => {

      val previousState: STATE = maybePreviousState.getOrElse(Nil).filter(_._3 == false)

      val mergeIn = newValuesInInterval.filter(_._2._1 == MergeInfo.PREV)
      val createNew = newValuesInInterval.filter(_._2._1 == MergeInfo.NEXT)
      val completeCurrentState_? = createNew.size > 0 ||
        newValuesInInterval.exists(_._2._1 == MergeInfo.FIN)

      val newState = if (completeCurrentState_?) {
        previousState.map(kv => (kv._1, kv._2, true)) ++
          mergeIn.map(kv => (kv._1, kv._2._2, true)) ++
          createNew.map(kv => (kv._1, kv._2._2, false))
      } else {
        // we know from the definition of completeCurrentState_? that
        // createNew is empty
        previousState ++ mergeIn.map(kv => (kv._1, kv._2._2, false))
      }
      Some(newState)
    }
    val state = allPartialWindows.updateStateByKey(stateUpdateFunction)

    /*
     * 6) Union the "can be processed" items in the state stream and the
     *    "complete windows" from each substream to obtain the final result.
     */

    val justCompletedWindows = state.flatMap(substreamAndState => {
      substreamAndState._2
    }).filter(stateItem =>
      // take only the completed items (with `true` flag)
      stateItem._3
      ).map(kv => (kv._1, kv._2))

    justCompletedWindows.union(allCompleteWindows)
  }

  /**
   * Computes a stateful DStream with the number of total counts in the stream.
   */
  private def slidingTotalItemCount[T](data: DStream[T]): DStream[ItemCountPair] = {
    // data
    // val:  a | b | c | d | e | f | g | h | i  ||  j | k | l | m | n  ||  o

    val itemCounts: DStream[(Null, Long)] = data.count().map((null, _))
    // itemCounts
    // key:  null                               ||  null               ||  null
    // val:  9                                  ||  5                  ||  1

    val updateCountState: (Seq[Long], Option[ItemCountPair]) =>
      Option[ItemCountPair] = (newItemCounts, maybeOldState) => {
      val oldState = maybeOldState.getOrElse(ItemCountPair(0L, 0L))
      val newState = oldState.increaseBy(newItemCounts.head)
      Some(newState)
    }
    val result = itemCounts.updateStateByKey(updateCountState).map(_._2)
    result.cache()
    // result
    // val:  (0,9)                              ||  (9,14)             ||  (14,15)
    result
  }

  /**
   * Adds a globally increasing Long index to every item.
   */
  private def addGlobalIndex[T](data: DStream[T],
                                slidingCount: DStream[ItemCountPair]): DStream[(Long, T)] = {
    val locallyIndexedStream: DStream[(ItemKey, T)] =
      data.transform(_.zipWithIndex.map(_.swap))

    // Increase the local indexes to global ones by adding an offset
    // obtained using a take() output operation on the 1-element state
    // DStream. See
    // <http://stackoverflow.com/questions/28080296/how-to-add-an-increasing-integer-id-to-items-in-a-spark-dstream>
    // for concerns about the validity of this approach.
    val increaseRDDIndex: (RDD[(ItemKey, T)], RDD[ItemCountPair]) =>
      RDD[(ItemKey, T)] = (streamData, slidingCount) => {
      val offset = slidingCount.take(1).head.previousCount
      streamData.map(keyValue => (keyValue._1 + offset, keyValue._2))
    }

    locallyIndexedStream.transformWith(slidingCount, increaseRDDIndex)
    // result
    // key:  0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8  ||  9 | 10 | 11 | 12 | 13  ||  14
    // val:  a | b | c | d | e | f | g | h | i  ||  j |  k |  l |  m |  n  ||   o
  }
}

/**
 * A data structure indicating the total count of items seen so far as
 * well as the total count seen until the previous interval.
 */
case class ItemCountPair(previousCount: Long, currentCount: Long) {
  def increaseBy(diff: Long) = ItemCountPair(currentCount, currentCount + diff)
}

/**
 * A data structure indicating the maximum key of items seen so far
 * as well as the maximum key from the previous interval.
 */
case class MaxKeyPair(previousMaxKey: ItemKey, currentMaxKey: ItemKey) {
  def shiftTo(newVal: ItemKey) = MaxKeyPair(currentMaxKey, newVal)
}

object MergeInfo extends Enumeration {
  val PREV, // merge with previous
  NOW, // process now
  NEXT, // merge with next
  FIN = // dummy with meaning "mark partial window as complete"
    Value
}
