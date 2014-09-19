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

import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.matching.Regex
import org.apache.spark.streaming.dstream.{OrderedFileInputDStream, ConstantInputDStream, DStream}
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import kafka.serializer.StringDecoder
import scala.collection.mutable.Queue
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.catalyst.types.StructType
import org.json4s.JValue
import org.json4s.native.JsonMethods._

// "struct" holding the number of processed items, runtime in ms and largest seen id
case class ProcessingInformation(itemCount: Long, runtime: Long, maxId: Option[String])

class HybridProcessor(sc: SparkContext,
                      sqlc: SQLContext,
                      storageLocation: String,
                      streamLocations: List[String])
  extends LazyLogging {
  /*
   * We want to do processing of static data first, then continue with
   * stream data. Various approaches are thinkable:
   *  1. Create a HybridDStream as a subclass of InputDStream,
   *  2. create a HybridReceiver as a subclass of Receiver and turn
   *     it into a DStream by means of StreamingContext.receiveStream(),
   *  3. process static and stream data one after another using two
   *     different StreamingContexts.
   *
   * A receiver must implement onStart(), onStop() and write the received data
   * to Spark's pipeline from a separate thread using the store() method. This
   * works nicely with the existing receivers such as KafkaReceiver, but custom
   * code is necessary to work with HDFS files and it might be tough to get the
   * parallel reading done right.
   * <http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.receiver.Receiver>
   *
   * An InputDStream must implement start(), stop(), and compute(time) to generate
   * an RDD with data collected in a certain interval. However, there seems to be
   * a subtle difference between an InputDStream running on a driver and a
   * ReceiverInputDStream that runs a receiver on worker nodes. It seems difficult
   * to write one DStream class that gets the parallelism in HDFS and stream
   * processing right.
   * <http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dstream.InputDStream>
   * <http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dstream.ReceiverInputDStream>
   *
   * Therefore we use two different StreamingContexts with one doing processing
   * of static data, the other one streaming data.
   */

  require(streamLocations.size <= 1,
    "More than one stream location is not supported at the moment.")

  // find the number of workers available to us.
  val _runCmd = scala.util.Properties.propOrElse("sun.java.command", "")
  val _master = sc.getConf.get("spark.master", "")
  val numCoresRe = ".*--executor-cores ([0-9]+) --num-executors ([0-9]+).*".r
  val totalNumCores = _runCmd match {
    case numCoresRe(coresPerExecutor, numExecutors) =>
      coresPerExecutor.toInt * numExecutors.toInt
    case _ =>
      0
  }
  if (totalNumCores > 0)
    logger.debug("total number of cores: " + totalNumCores)
  else
    logger.warn("could not extract number of cores from run command: " + _runCmd)

  // define the formats that we can use
  val fileRe = """file://(.+)""".r
  val hdfsRe = """(hdfs://.+)""".r
  val kafkaRe = """kafka://([^/]+)/([^/]+)/([^/]+)$""".r
  val dummyRe = """^dummy(.?)""".r
  val emptyRe = """^empty(.?)""".r
  val validStaticLocations: List[Regex] = emptyRe :: fileRe :: hdfsRe :: Nil
  val validStreamLocations: List[Regex] = dummyRe :: kafkaRe :: Nil

  // check if storageLocation matches one of the valid regexes
  if (!validStaticLocations.exists(_.findFirstIn(storageLocation).isDefined)) {
    throw new IllegalArgumentException(s"'$storageLocation' is not a valid storage " +
      "specification")
  }
  // check if all given streamLocations match one of the valid regexes
  val badStreamLocations = streamLocations.filter(loc =>
    !validStreamLocations.exists(_.findFirstIn(loc).isDefined))
  badStreamLocations collectFirst {
    case loc =>
      throw new IllegalArgumentException(s"'$loc' is not a valid stream specification")
  }

  type IdType = String

  // Holds the current streaming context
  protected var ssc_ : StreamingContext = null

  def currentStreamingContext() = ssc_

  // Flag that stores whether static data processing completed successfully
  protected var staticProcessingComplete = false

  // Flag that stores whether user stopped data processing manually
  protected var userStoppedProcessing = false

  /**
   * Start hybrid processing using the given transformation.
   *
   * @param transform an RDD operation that will be performed on each batch
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  def start(transform: RDD[JValue] => RDD[_]): (() => (ProcessingInformation, ProcessingInformation),
    () => Option[IdType]) = {
    val parseJsonStringIntoOption: (String => Traversable[JValue]) = line => {
      val maybeJson = parseOpt(line)
      if (maybeJson.isEmpty) {
        // logger is not serializable, therefore use println
        println("[ERROR] unparseable JSON: " + line)
      }
      maybeJson
    }
    val parseAndTransform: RDD[String] => RDD[Unit] = rdd => {
      transform(rdd.flatMap(parseJsonStringIntoOption)).map(_ => ())
    }
    _start(parseAndTransform)
  }

  /**
   * Start hybrid processing using the given transformation.
   *
   * @param transform an RDD operation that will be performed on each batch
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  def start(transform: SchemaRDD => SchemaRDD,
            schema: Option[StructType]): (() => (ProcessingInformation, ProcessingInformation),
    () => Option[IdType]) = {
    val parseAndTransform: RDD[String] => RDD[Unit] = rdd => {
      // with an empty RDD, we cannot infer the schema (it will raise an exception)
      if (rdd.count() > 0) {
        // parse with schema or infer if not given
        val jsonRdd = schema.map(sqlc.jsonRDD(rdd, _)).getOrElse(sqlc.jsonRDD(rdd, 0.1))
        transform(jsonRdd).map(_ => ())
      } else {
        // create an (empty) SchemaRDD
        rdd.map(_ => ())
      }
    }
    _start(parseAndTransform)
  }

  /**
   * Start hybrid processing using the given transformation.
   *
   * @param parseAndTransform an RDD operation that will be performed on each batch
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  protected def _start(parseAndTransform: RDD[String] => RDD[Unit]):
  (() => (ProcessingInformation, ProcessingInformation), () => Option[IdType]) = {
    logger.debug("creating StreamingContext for static data")
    ssc_ = new StreamingContext(sc, Seconds(2))

    // this has to match our jubaql_timestamp inserted by fluentd
    val extractRe = """.+"jubaql_timestamp": ?"([0-9\-:.T]+)".*""".r

    val extractId: String => IdType = item => {
      item match {
        case extractRe(idString) =>
          idString
        case _ =>
          ""
      }
    }

    // create the static data source
    val staticData: DStream[String] = storageLocation match {
      /* Notes:
       * 1. We have to use fileStream instead of textFileStream because we need
       *    to pass in newFilesOnly=false.
       * 2. The implementation of fileStream will process all existing files
       *    in the first batch (which will maybe take a very long time).  If
       *    a new file appears during that processing, it will be added to the
       *    batch of the time when it appeared.  It may be worth considering a
       *    different implementation using a Queue that only enqueues new files
       *    when all previous processing is done, but we need to closely examine
       *    the behavior for very long batch processing times before deciding
       *    on that.
       * 3. We have no guarantee about the order of files when using the standard
       *    FileInputDStream, since it uses o.a.h.f.FileSystem.listStatus() under
       *    the hood (that is knowledge we should not actually use) and there
       *    doesn't seem to be any contract about order of files.  Our custom
       *    OrderedFileInputDStream adds that ordering.
       * 4. Files that are currently being appended to seem to be read as well
       *    by the standard FileInputDStream.  We do *not* want that, since
       *    such a file would be marked as "processed" and the next file that
       *    appears would be picked up, even though we did not process all
       *    its contents.  Therefore we use a custom OrderedFileInputDStream
       *    that ignores files that received updates recently.
       */
      case emptyRe(something) =>
        val queue: Queue[RDD[String]] = new Queue()
        ssc_.queueStream(queue)
      case fileRe(filepath) =>
        val realpath = if (filepath.startsWith("/")) {
          filepath
        } else {
          (new File(".")).getAbsolutePath + "/" + filepath
        }
        new OrderedFileInputDStream[LongWritable, Text, TextInputFormat](ssc_,
          "file://" + realpath,
          (path: Path) => true,
          false).map(_._2.toString)
      case hdfsRe(filepath) =>
        new OrderedFileInputDStream[LongWritable, Text, TextInputFormat](ssc_,
          filepath,
          (path: Path) => true,
          false).map(_._2.toString)
    }
    logger.debug("static data DStream: " + staticData)

    // keep track of the maximal ID seen during processing
    val maxStaticId = sc.accumulator[Option[IdType]](None)(new MaxOptionAccumulatorParam[IdType])
    val countStatic = sc.accumulator(0L)
    val maxStreamId = sc.accumulator[Option[IdType]](None)(new MaxOptionAccumulatorParam[IdType])
    val countStream = sc.accumulator(0L)

    // processing of static data
    val repartitionedData = if (_master == "yarn-cluster" && totalNumCores > 0) {
      // We repartition by (numExecutors * executorCores) to get just the
      // right level of parallelism.
      logger.info(s"repartitioning for $totalNumCores workers")
      staticData.repartition(totalNumCores)
    } else {
      logger.debug("not repartitioning")
      staticData
    }
    repartitionedData.map(item => {
      // update maximal ID
      maxStaticId += Some(extractId(item))
      item
    }).transform(parseAndTransform).foreachRDD(rdd => {
      val count = rdd.count()
      // we count the number of total processed rows (on the driver)
      countStatic += count
      // stop processing of static data if there are no new files
      if (count == 0) {
        logger.info(s"processed $count (static) lines, looks like done")
        synchronized {
          staticProcessingComplete = true
        }
      } else {
        logger.info(s"processed $count (static) lines")
      }
    })

    // start first StreamingContext
    logger.info("starting static data processing")
    val staticStartTime = System.currentTimeMillis()
    var staticRunTime = 0L
    var streamStartTime = -1L
    var streamRunTime = 0L
    ssc_.start()
    val staticStreamingContext = ssc_

    // start one thread that waits for static data processing to complete
    future {
      logger.debug("hello from thread to wait for completion of static processing")
      // If *either* the static data processing completed successfully,
      // *or* the staticStreamingContext finished for some other reason
      // (we measure this by the execution time of awaitTermination(timeout))
      // we stop the streaming context.
      val timeToWait = 200L
      val logEveryNLoops = 5
      var i = 0
      var staticProcessingStillRunning = true
      while (!staticProcessingComplete && staticProcessingStillRunning) {
        val timeBeforeWaiting = System.currentTimeMillis()
        if (i == logEveryNLoops) {
          logger.debug("waiting for static data processing to complete")
          i = 0
        } else {
          i += 1
        }
        staticStreamingContext.awaitTermination(timeToWait)
        val timeAfterWaiting = System.currentTimeMillis()
        val actuallyWaitedTime = timeAfterWaiting - timeBeforeWaiting
        staticProcessingStillRunning = actuallyWaitedTime >= timeToWait
      }
      if (staticProcessingComplete) {
        logger.info("static data processing completed successfully, " +
          "stopping StreamingContext")
      } else {
        logger.warn("static data processing ended, but did not complete")
      }
      staticStreamingContext.stop(false, true)
      logger.debug("bye from thread to wait for completion of static processing")
    } onFailure {
      case error: Throwable =>
        logger.error("Error while waiting for static processing end", error)
    }

    // start one thread that waits for the first StreamingContext to terminate
    future {
      // NB. This is a separate thread. In functions that will be serialized,
      // you cannot necessarily use variables from outside this thread.
      // Also see <http://mail-archives.apache.org/mod_mbox/spark-user/201501.mbox/%3CCANGvG8pf+ukzLi38hjVeV91BVW0zgEFB4KX2niK+BW55M_zNFw@mail.gmail.com%3E>.
      val localExtractId = extractId
      val localCountStream = countStream
      val localMaxStreamId = maxStreamId
      logger.debug("hello from thread to start stream processing")
      staticStreamingContext.awaitTermination()
      // If we arrive here, the static processing is done, either by failure
      // or user termination or because all processing was completed. We want
      // to continue with real stream processing only if the static processing
      // was completed successfully.
      val largestStaticItemId = maxStaticId.value
      staticRunTime = System.currentTimeMillis() - staticStartTime
      logger.debug("static processing ended after %d items and %s ms, largest seen ID: %s".format(
        countStatic.value, staticRunTime, largestStaticItemId))
      if (staticProcessingComplete && !userStoppedProcessing) {
        logger.info("static processing completed successfully, setting up stream")
        streamLocations match {
          case streamLocation :: Nil =>
            // set up stream processing
            logger.debug("creating StreamingContext for stream data")
            ssc_ = new StreamingContext(sc, Seconds(2))
            val allStreamData: DStream[String] = streamLocation match {
              case dummyRe(nothing) =>
                // dummy JSON data emitted over and over
                val dummyData = sc.parallelize("{\"id\": 5}" :: "{\"id\": 6}" ::
                  "{\"id\": 7}" :: Nil)
                new ConstantInputDStream(ssc_, dummyData)
              case kafkaRe(zookeeper, topics, groupId) =>
                // connect to the given Kafka instance and receive data
                val kafkaParams = Map[String, String](
                  "zookeeper.connect" -> zookeeper, "group.id" -> groupId,
                  "auto.offset.reset" -> "smallest")
                KafkaUtils.createStream[String, String,
                  StringDecoder, StringDecoder](ssc_, kafkaParams,
                    Map(topics -> 2),
                    // With MEMORY_ONLY, we seem to run out of memory quickly
                    // when processing is slow. Much worse: There is no space
                    // left for broadcast variables, so we cannot communicate
                    // our "runState = false" information.
                    StorageLevel.DISK_ONLY).map(_._2)
            }
            val streamData = (largestStaticItemId match {
              case Some(largestId) =>
                // only process items with a strictly larger id than what we
                // have seen so far
                logger.info("filtering for items with an id larger than " + largestId)
                allStreamData.filter(item => {
                  localExtractId(item) > largestId
                })
              case None =>
                // don't do any ID filtering if there is no "largest id"
                logger.info("did not see any items in static processing, " +
                  "processing whole stream")
                allStreamData
            }).map(item => {
              // remember the largest seen ID
              localMaxStreamId += Some(localExtractId(item))
              item
            })
            logger.debug("stream data DStream: " + streamData)
            streamData.transform(parseAndTransform).foreachRDD(rdd => {
              // this `count` is *necessary* to trigger the (lazy) transformation!
              val count = rdd.count()
              // we count the number of total processed rows (on the driver)
              localCountStream += count
              logger.info(s"processed $count (stream) lines")
            })
            // start stream processing
            synchronized {
              if (userStoppedProcessing) {
                logger.info("processing was stopped by user during stream setup, " +
                  "not starting")
              } else {
                logger.info("starting stream processing")
                streamStartTime = System.currentTimeMillis()
                ssc_.start()
              }
            }
          case Nil =>
            logger.info("not starting stream processing " +
              "(no stream source given)")
          case _ =>
            logger.error("not starting stream processing " +
              "(multiple streams not implemented)")
        }
      } else if (staticProcessingComplete && userStoppedProcessing) {
        logger.info("static processing was stopped by user, " +
          "not setting up stream")
      } else {
        logger.warn("static processing did not complete successfully, " +
          "not setting up stream")
      }
      logger.debug("bye from thread to start stream processing")
    } onFailure {
      case error: Throwable =>
        logger.error("Error while setting up stream processing", error)
    }

    // return a function to stop the data processing
    (() => {
      logger.info("got shutdown request from user")
      synchronized {
        userStoppedProcessing = true
      }
      logger.debug("now stopping the StreamingContext")
      currentStreamingContext.stop(false, true)
      logger.debug("done stopping the StreamingContext")
      // if stream processing was not started or there was a runtime already
      // computed, we don't update the runtime
      if (streamStartTime > 0 && streamRunTime == 0) {
        streamRunTime = System.currentTimeMillis() - streamStartTime
      }
      logger.info(("processed %s items in %s ms (static) and %s items in " +
        "%s ms (stream)").format(countStatic.value, staticRunTime,
          countStream.value, streamRunTime))
      (ProcessingInformation(countStatic.value, staticRunTime, maxStaticId.value),
        ProcessingInformation(countStream.value, streamRunTime, maxStreamId.value))
    }, () => maxStaticId.value)
  }


  def awaitTermination() = {
    ssc_.awaitTermination()
  }
}
