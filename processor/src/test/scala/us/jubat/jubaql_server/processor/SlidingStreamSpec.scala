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
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.format.ISODateTimeFormat
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

class SlidingStreamSpec extends FeatureSpec
with GivenWhenThen
with ShouldMatchers
with BeforeAndAfterAll {
  val sc = new SparkContext("local[3]", "SlidingWindow")
  sc.setCheckpointDir("file:///tmp/spark")
  var ssc: StreamingContext = null

  feature("Count-based sliding windows") {
    ssc = new StreamingContext(sc, Seconds(1))

    Given("a simple DStream")
    type T = Char
    val rawData = "abcd" :: "ef" :: "ghijklmn" :: "o" :: "pq" :: "rstuvwxyz" :: Nil
    val rawDataQueue = new mutable.Queue[List[T]]()
    rawData.foreach(rawDataQueue += _.toList)
    val itemsQueue = rawDataQueue.map(s => sc.parallelize(s.toList))
    val inputStream: DStream[T] = ssc.queueStream(itemsQueue, oneAtATime = true)

    // set up sliding window streams for various window parameters.
    // (we need to set this up before the scenario() block to compute
    // all of them within the same StreamingContext run...)
    val streams = for (length <- 1 to 4;
                       step <- Math.max(1, length - 1) to (length + 1)) yield {
      val windowStream = SlidingWindow.byCount(inputStream, length, step)
      // add the computed windows in each interval to a mutable list
      // for analysis
      val windowsPerInterval = mutable.ListBuffer[List[(Long, List[(Long, T)])]]()
      windowStream.foreachRDD(rdd => {
        val windows = rdd.groupByKey().mapValues(_.toList).collect().toList
        windowsPerInterval += windows
      })
      (length, step, windowsPerInterval)
    }

    waitUntilProcessingEnds(inputStream, rawData.size)

    for ((length, step, windowsPerInterval) <- streams) {
      checkCountResult(length, step, windowsPerInterval, rawData.map(_.toList))
    }
  }


  feature("Count-based sliding windows (weird data distribution)") {
    ssc = new StreamingContext(sc, Seconds(1))

    Given("a weird DStream")
    type T = Char
    val rawData = "ab" :: "" :: "cde" :: "f" :: "ghijklmnopqrstuvwxy" :: "z" :: Nil
    val rawDataQueue = new mutable.Queue[List[T]]()
    rawData.foreach(rawDataQueue += _.toList)
    val itemsQueue = rawDataQueue.map(s => sc.parallelize(s.toList))
    val inputStream: DStream[T] = ssc.queueStream(itemsQueue, oneAtATime = true)

    // set up sliding window streams for various window parameters.
    // (we need to set this up before the scenario() block to compute
    // all of them within the same StreamingContext run...)
    val streams = for (length <- 4 to 4;
                       step <- 1 to (length + 1)) yield {
      val windowStream = SlidingWindow.byCount(inputStream, length, step)
      // add the computed windows in each interval to a mutable list
      // for analysis
      val windowsPerInterval = mutable.ListBuffer[List[(Long, List[(Long, T)])]]()
      windowStream.foreachRDD(rdd => {
        val windows = rdd.groupByKey().mapValues(_.toList).collect().toList
        windowsPerInterval += windows
      })
      (length, step, windowsPerInterval)
    }

    waitUntilProcessingEnds(inputStream, rawData.size)

    for ((length, step, windowsPerInterval) <- streams) {
      checkCountResult(length, step, windowsPerInterval, rawData.map(_.toList))
    }
  }


  feature("Timestamp-based sliding windows") {
    ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("file:///tmp/spark")

    Given("a simple DStream")
    val parser = ISODateTimeFormat.dateHourMinuteSecondFraction()
    type T = String
    val rawDataUnparsed =
      List(("a", "2015-01-09T16:23:34.031128377"), // 1420788214031
        ("b", "2015-01-09T16:23:34.035617484"), // 1420788214035
        ("c", "2015-01-09T16:23:34.132088288"), // 1420788214132
        ("d", "2015-01-09T16:23:35.136510729"), // 1420788215136
        ("e", "2015-01-09T16:23:36.004067229"), // 1420788216004
        ("f", "2015-01-09T16:23:36.039922085"), // 1420788216039
        ("g", "2015-01-09T16:23:36.106793425"), // 1420788216106
        ("h", "2015-01-09T16:23:36.140707388"), // 1420788216140
        ("i", "2015-01-09T16:23:38.010804037")) :: // 1420788218010
        List(("j", "2015-01-09T16:23:38.111558838"), // 1420788218111
          ("k", "2015-01-09T16:23:40.015171109"), // 1420788220015
          ("l", "2015-01-09T16:23:40.020104192"), // 1420788220020
          ("m", "2015-01-09T16:23:40.116416331"), // 1420788220116
          ("n", "2015-01-09T16:23:40.121018469")) :: // 1420788220121
        List() ::
        List(("o", "2015-01-09T16:23:41.024491603")) :: // 1420788221024
        List(("p", "2015-01-09T16:23:41.125453953"), // 1420788221125
          ("q", "2015-01-09T16:23:42.029155492"), // 1420788222029
          ("r", "2015-01-09T16:23:42.129897677"), // 1420788222129
          ("s", "2015-01-09T16:23:43.033451709"), // 1420788223033
          ("t", "2015-01-09T16:23:43.134198126"), // 1420788223134
          ("u", "2015-01-09T16:23:44.037491118"), // 1420788224037
          ("v", "2015-01-09T16:23:44.138379275")) :: // 1420788224138
        List(("w", "2015-01-09T16:23:45.001737653"), // 1420788225001
          ("x", "2015-01-09T16:23:45.102486660"), // 1420788225102
          ("y", "2015-01-09T16:23:46.006160454"), // 1420788226006
          ("z", "2015-01-09T16:23:46.107519895")) :: // 1420788226107
        Nil
    val rawData = rawDataUnparsed.map(_.map(item => {
      val (data, timestamp) = item
      (parser.parseMillis(timestamp), data)
    }))
    val rawDataQueue = new mutable.Queue[List[(Long, T)]]()
    rawData.foreach(rawDataQueue += _)
    val itemsQueue = rawDataQueue.map(s => sc.parallelize(s))
    val inputStream: DStream[(Long, T)] = ssc.queueStream(itemsQueue, oneAtATime = true)

    // set up sliding window streams for various window parameters.
    // (we need to set this up before the scenario() block to compute
    // all of them within the same StreamingContext run...)
    val streams = for (length <- 3 to 4;
                       step <- Math.max(1, length - 1) to (length + 1)) yield {
      //val step = 3
      val windowStream = SlidingWindow.byTimestamp(inputStream, length, step)
      // add the computed windows in each interval to a mutable list
      // for analysis
      val windowsPerInterval = mutable.ListBuffer[List[(Long, List[(Long, T)])]]()
      windowStream.foreachRDD(rdd => {
        val windows = rdd.groupByKey().mapValues(_.toList).collect().toList
        windowsPerInterval += windows
      })
      (length, step, windowsPerInterval)
    }

    waitUntilProcessingEnds(inputStream, rawData.size)

    for ((length, step, windowsPerInterval) <- streams) {
      checkTimestampResult(length * 1000, step * 1000, windowsPerInterval, rawData)
    }
  }


  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  protected def checkCountResult[T](length: Int, step: Int,
                                    windowsPerInterval: Seq[List[(Long, List[(Long, T)])]],
                                    rawData: List[List[T]]) = {
    scenario(s"Window length $length and step size $step") {
      When("we compute sliding windows by count")

      Then("the group indexes in every interval should be continuous")
      val minMaxIndexes = windowsPerInterval.filterNot(_.isEmpty).map(windows => {
        val groupIndexes = windows.map(_._1)
        val minGroupIndex = groupIndexes.min
        val maxGroupIndex = groupIndexes.max
        // an interval should contain all group indexes between min and max group
        // index (i.e., there should be no gaps within an interval)
        groupIndexes.sorted shouldBe ((minGroupIndex to maxGroupIndex).toList)
        (minGroupIndex, maxGroupIndex)
      })

      And("they should increase between intervals")
      // i.e. there should be no gaps between intervals
      minMaxIndexes.take(minMaxIndexes.size - 1).zip(minMaxIndexes.tail).foreach(pair => {
        val (previousIndexes, currentIndexes) = pair
        previousIndexes._2 + 1 shouldBe (currentIndexes._1)
      })

      And("all groups should have the correct elements")
      val sortedWindows = windowsPerInterval.reduceLeft(_ ++ _).sortBy(_._1)
      val ourSlidingWindows: List[Seq[T]] = sortedWindows.map(groupWithIdx =>
        groupWithIdx._2.toSeq.sortBy(_._1).map(_._2))
      // compare with the iterator.sliding() results
      val slidingIterator = rawData.reduceLeft(_ ++ _).iterator.sliding(length, step)
      val completeWindows = slidingIterator.filter(_.size == length).toList
      // (maybe) drop last window because that will never be completed
      // in our implementation
      val referenceSlidingWindows = completeWindows.take(ourSlidingWindows.size)
      ourSlidingWindows shouldBe (referenceSlidingWindows)

      info(ourSlidingWindows.toString().take(40) + " ...")
    }
  }

  protected def checkTimestampResult[T](length: Int, step: Int,
                                        windowsPerInterval: Seq[List[(Long, List[(Long, T)])]],
                                        rawData: List[List[(Long, T)]]) = {
    scenario(s"Window length $length and step size $step") {
      When("we compute sliding windows by count")

      Then("the group timestamps should increase between intervals")
      val minMaxTimestamps = windowsPerInterval.filterNot(_.isEmpty).map(windows => {
        val groupTimestamps = windows.map(_._1)
        (groupTimestamps.min, groupTimestamps.max)
      })
      minMaxTimestamps.take(minMaxTimestamps.size - 1).zip(minMaxTimestamps.tail).foreach(pair => {
        val (previousTimestamps, currentTimestamps) = pair
        previousTimestamps._2 shouldBe <(currentTimestamps._1)
      })

      And("all groups should have the correct elements")
      val sortedWindows = windowsPerInterval.reduceLeft(_ ++ _).sortBy(_._1)
      val ourSlidingWindows: List[(Long, Seq[(Long, T)])] = sortedWindows.map(groupWithIdx =>
        (groupWithIdx._1, groupWithIdx._2.sortBy(_._1)))

      // compare with an inefficient, but probably correct implementation
      val allRawData = rawData.reduceLeft(_ ++ _).sortBy(_._1)
      val batchSize = ((length - 1) / step + 1) * step
      val minGroupTimestamp = allRawData.map(_._1).min / batchSize * batchSize
      val maxGroupTimestamp = allRawData.map(_._1).max / batchSize * batchSize
      val slidingIterator =
        (for (groupTimestamp <- minGroupTimestamp to maxGroupTimestamp by step) yield {
          val items = allRawData.filter(kv =>
            kv._1 >= groupTimestamp && kv._1 < groupTimestamp + length
          )
          (groupTimestamp, items)
        }).filterNot(_._2.isEmpty).toList

      // (maybe) drop last window because that will never be completed
      // in our implementation
      val referenceSlidingWindows = slidingIterator.take(ourSlidingWindows.size)
      ourSlidingWindows shouldBe (referenceSlidingWindows)
    }
  }

  protected def waitUntilProcessingEnds(stream: DStream[_], numIterations: Int) = {
    // count up in every interval
    val i = sc.accumulator(0)
    stream.foreachRDD(rdd => i += 1)
    // start processing
    ssc.start()
    // stop streaming context when i has become numIterations
    future {
      while (i.value < numIterations + 1)
        Thread.sleep(100)
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
    // wait for termination
    ssc.awaitTermination()
  }
}
