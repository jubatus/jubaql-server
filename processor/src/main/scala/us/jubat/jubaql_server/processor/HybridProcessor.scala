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

import RunMode.Development

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
import scala.collection.mutable.{Queue, LinkedHashMap}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.catalyst.types.StructType
import org.json4s.JValue
import org.json4s.native.JsonMethods._

// "struct" holding the number of processed items, runtime in ms and largest seen id
case class ProcessingInformation(itemCount: Long, runtime: Long, maxId: Option[String])

// an object describing the state of the processor
sealed trait ProcessorState

case object Initialized extends ProcessorState

case object Running extends ProcessorState

case object Finished extends ProcessorState

// an object describing the phase of the processing
sealed abstract class ProcessingPhase(val name: String) {
  def getPhaseName(): String = { name }
}

case object StopPhase extends ProcessingPhase("Stop")

case object StoragePhase extends ProcessingPhase("Storage")

case object StreamPhase extends ProcessingPhase("Stream")

class HybridProcessor(sc: SparkContext,
                      sqlc: SQLContext,
                      storageLocation: String,
                      streamLocations: List[String],
                      runMode: RunMode = RunMode.Development,
                      checkpointDir: String = "file:///tmp/spark")
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

  /// define the STORAGE sources that we can use
  // a file in the local file system (must be accessible by all executors)
  val fileRe = """file://(.+)""".r
  // a file in HDFS
  val hdfsRe = """(hdfs://.+)""".r
  // an empty data set
  val emptyRe = """^empty(.?)""".r
  /// define the STREAM sources that we can use
  // a Kafka message broker (host:port/topic/groupid)
  val kafkaRe = """kafka://([^/]+)/([^/]+)/([^/]+)$""".r
  // endless dummy JSON data
  val dummyRe = """^dummy(.?)""".r
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

  // state of the processor
  protected var _state: ProcessorState = Initialized

  protected def setState(newState: ProcessorState) = synchronized {
    _state = newState
  }

  def state: ProcessorState = synchronized {
    _state
  }

  // phase of the processing
  protected var _phase: ProcessingPhase = StopPhase

  protected def setPhase(newPhase: ProcessingPhase) = synchronized {
    _phase = newPhase
  }

  def phase: ProcessingPhase = synchronized {
    _phase
  }

  // latest time-stamp
  var latestStaticTimestamp = sc.accumulator[Option[IdType]](None)(new MaxOptionAccumulatorParam[IdType])
  var latestStreamTimestamp = sc.accumulator[Option[IdType]](None)(new MaxOptionAccumulatorParam[IdType])

  var storageCount = sc.accumulator(0L)
  var streamCount = sc.accumulator(0L)
  var staticStartTime = 0L
  var streamStartTime = 0L

  /**
   * Start hybrid processing using the given RDD[JValue] operation.
   *
   * The stream data will be parsed into a JValue (if possible) and the
   * transformation is expected to act on the resulting RDD[JValue].
   * Note that *as opposed to* the `start(SchemaRDD => SchemaRDD)` version,
   * if the input RDD is empty, the function will still be executed.
   *
   * @param process an RDD operation that will be performed on each batch
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  def startJValueProcessing(process: RDD[JValue] => Unit): (() => (ProcessingInformation, ProcessingInformation),
    () => Option[IdType]) = {
    val parseJsonStringIntoOption: (String => Traversable[JValue]) = line => {
      val maybeJson = parseOpt(line)
      if (maybeJson.isEmpty) {
        // logger is not serializable, therefore use println
        println("[ERROR] unparseable JSON: " + line)
      }
      maybeJson
    }
    // parse DStream[String] into DStream[JValue] item by item,
    // skipping unparseable strings
    val parseJsonDStream = (stream: DStream[String]) =>
      stream.flatMap(parseJsonStringIntoOption)
    val processJsonDStream: DStream[JValue] => Unit =
      _.foreachRDD(process)
    // start processing
    _start(parseJsonDStream, processJsonDStream)
  }

  /**
   * Start hybrid processing using the given SchemaRDD operation.
   *
   * The stream data will be equipped with a schema (either as passed
   * as a parameter or as inferred by `SQLContext.jsonRDD()`) and the
   * operation is expected to act on the resulting SchemaRDD.
   * Note that if the RDD is empty, the given function will not be
   * executed at all (not even with an empty RDD as a parameter).
   *
   * @param process an RDD operation that will be performed on each batch
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  def startTableProcessing(process: SchemaRDD => Unit,
            schema: Option[StructType]): (() => (ProcessingInformation, ProcessingInformation),
    () => Option[IdType]) = {
    // parse DStream[String] into a row/column shaped stream
    val parseJson: DStream[String] => SchemaDStream = schema match {
      case Some(givenSchema) =>
        SchemaDStream.fromStringStreamWithSchema(sqlc, _, givenSchema, None)
      case None =>
        SchemaDStream.fromStringStream(sqlc, _, None)
    }
    // We must only execute the process function if the RDD is non-empty.
    // For inferred schema method, if the RDD is empty then the schema
    // will be empty, too. For given schema method, we have to check
    // the actual count (which is more expensive).
    val processIfNotEmpty: SchemaRDD => Unit = schema match {
      case Some(givenSchema) =>
        rdd => if (rdd.count() > 0) process(rdd)
      case None =>
        rdd => if (rdd.schema.fields.size > 0) process(rdd)
    }
    val processStream: SchemaDStream => Unit =
      _.foreachRDD(processIfNotEmpty)
    _start[SchemaDStream](parseJson, processStream)
  }

  /**
   * Start hybrid processing using the given SchemaRDD operation.
   *
   * The stream data will be equipped with a schema (either as passed
   * as a parameter or as inferred by `SQLContext.jsonRDD()`) and the
   * operation is expected to act on the resulting SchemaDStream.
   * The function is responsible for triggering output operations.
   *
   * @param process a function to transform and operate on the main DStream
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  def startTableProcessingGeneral(process: SchemaDStream => Unit,
                                  schema: Option[StructType],
                                  inputStreamName: String): (() => (ProcessingInformation,
    ProcessingInformation), () => Option[IdType]) = {
    // parse DStream[String] into a row/column shaped stream
    val parseJson: DStream[String] => SchemaDStream = schema match {
      case Some(givenSchema) =>
        SchemaDStream.fromStringStreamWithSchema(sqlc, _, givenSchema, Some(inputStreamName))
      case None =>
        SchemaDStream.fromStringStream(sqlc, _, Some(inputStreamName))
    }
    _start[SchemaDStream](parseJson, process)
  }

  /**
   * Start hybrid processing using the given operation.
   *
   * The function passed in must operate on an RDD[String] (the stream data
   * to be processed in a single batch), where each item of the RDD can be
   * assumed to be JSON-encoded. The function *itself* is responsible to
   * start computation (e.g. by using `rdd.foreach()` or `rdd.count()`).
   * As that function can do arbitrary (nested and chained) processing, the
   * notion of "number of processed items" makes only limited sense; we
   * work with the "number of input items" instead.
   *
   * @param parseJson a function to get the input stream into something processable,
   *                  like `DStream[String] => DStream[JValue]` or
   *                  `DStream[String] => SchemaDStream`. "processable" means
   *                  that there is a `foreachRDD()` method matching the
   *                  parameter type of the `process()` function.
   *                  (This is applied duck typing!)
   * @param process the actual operations on the parsed data stream. Note that
   *                this function is responsible for calling an output operation.
   * @tparam T the type of RDD that the parsed stream will allow processing on,
   *           like `RDD[JValue]` or `SchemaRDD`
   * @return one function to stop processing and one to get the highest IDs seen so far
   */
  protected def _start[T](parseJson: DStream[String] => T,
                          process: T => Unit):
  (() => (ProcessingInformation, ProcessingInformation), () => Option[IdType]) = {
    if (state != Initialized) {
      val msg = "processor cannot be started in state " + state
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    setState(Running)
    logger.debug("creating StreamingContext for static data")
    /* In order for updateStreamByKey() to work, we need to enable RDD checkpointing
     * by setting a checkpoint directory. Note that this is different from enabling
     * Streaming checkpointing (which would be needed for driver fault-tolerance),
     * which would require the whole state of the application (in particular, all
     * functions in stream.foreachRDD(...) calls) to be serializable. This would
     * mean a rewrite of large parts of code, if it is possible at all.
     * Also see <https://www.mail-archive.com/user%40spark.apache.org/msg22150.html>.
     */
    sc.setCheckpointDir(checkpointDir)
    ssc_ = new StreamingContext(sc, Seconds(2))

    // this has to match our jubaql_timestamp inserted by fluentd
    val timestampInJsonRe = """ *"jubaql_timestamp": ?"([0-9\-:.T]+)" *""".r

    // Extract a jubaql_timestamp field from a JSON-shaped string and return it.
    val extractId: String => IdType = item => {
      timestampInJsonRe.findFirstMatchIn(item) match {
        case Some(aMatch) =>
          val id = aMatch.group(1)
          id
        case None =>
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
    latestStaticTimestamp = maxStaticId
    storageCount = sc.accumulator(0L)
    val maxStreamId = sc.accumulator[Option[IdType]](None)(new MaxOptionAccumulatorParam[IdType])
    streamCount = sc.accumulator(0L)

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
    // first find the maximal ID in the data and count it
    repartitionedData.map(item => {
      val id = extractId(item)
      // update maximal ID
      maxStaticId += Some(id)
      id
    }).foreachRDD(rdd => {
      val count = rdd.count()
      // we count the number of total input rows (on the driver)
      storageCount += count
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
    // now do the actual processing
    val mainStream = parseJson(repartitionedData)
    process(mainStream)

    // start first StreamingContext
    logger.info("starting static data processing")
    staticStartTime = System.currentTimeMillis()
    var staticRunTime = 0L
    var streamRunTime = 0L
    setPhase(StoragePhase)

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
      staticStreamingContext.stop(stopSparkContext = false, stopGracefully = true)
      logger.debug("bye from thread to wait for completion of static processing")
    } onFailure {
      case error: Throwable =>
        logger.error("Error while waiting for static processing end", error)
    }

    // start one thread that waits for the first StreamingContext to terminate
    future {
      // NB. This is a separate thread. In functions that will be serialized,
      // you cannot necessarily use variables from outside this thread.
      val localExtractId = extractId
      val localCountStream = streamCount
      val localMaxStreamId = maxStreamId
      logger.debug("hello from thread to start stream processing")
      staticStreamingContext.awaitTermination()
      // If we arrive here, the static processing is done, either by failure
      // or user termination or because all processing was completed. We want
      // to continue with real stream processing only if the static processing
      // was completed successfully.
      val largestStaticItemId = maxStaticId.value
      latestStreamTimestamp = maxStreamId

      staticRunTime = System.currentTimeMillis() - staticStartTime
      logger.debug("static processing ended after %d items and %s ms, largest seen ID: %s".format(
        storageCount.value, staticRunTime, largestStaticItemId))
      logger.debug("sleeping a bit to allow Spark to settle")
      runMode match {
        case Development =>
          Thread.sleep(200)
        case _ =>
          // If we don't sleep long enough here, then old/checkpointed RDDs
          // won't be cleaned up in time before the next process starts. For
          // some reason, this happens only with YARN.
          Thread.sleep(8000)
      }
      if (staticProcessingComplete && !userStoppedProcessing) {
        logger.info("static processing completed successfully, setting up stream")
        streamLocations match {
          case streamLocation :: Nil =>
            // set up stream processing
            logger.debug("creating StreamingContext for stream data")
            ssc_ = new StreamingContext(sc, Seconds(2))
            val allStreamData: DStream[(IdType, String)] = (streamLocation match {
              case dummyRe(nothing) =>
                // dummy JSON data emitted over and over (NB. the timestamp
                // is not increasing over time)
                val dummyData = sc.parallelize(
                  """{"gender":"m","age":26,"jubaql_timestamp":"2014-11-21T15:52:21.943321112"}""" ::
                    """{"gender":"f","age":24,"jubaql_timestamp":"2014-11-21T15:52:22"}""" ::
                    """{"gender":"m","age":31,"jubaql_timestamp":"2014-11-21T15:53:21.12345"}""" ::
                    Nil)
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
            }).map(item => (localExtractId(item), item))
            val streamData = (largestStaticItemId match {
              case Some(largestId) =>
                // only process items with a strictly larger id than what we
                // have seen so far
                logger.info("filtering for items with an id larger than " + largestId)
                allStreamData.filter(itemWithId => {
                  itemWithId._1 > largestId
                })
              case None =>
                // don't do any ID filtering if there is no "largest id"
                logger.info("did not see any items in static processing, " +
                  "processing whole stream")
                allStreamData
            }).map(itemWithId => {
              // remember the largest seen ID
              localMaxStreamId += Some(itemWithId._1)
              itemWithId._2
            })
            logger.debug("stream data DStream: " + streamData)
            streamData.foreachRDD(rdd => {
              val count = rdd.count()
              // we count the number of total processed rows (on the driver)
              localCountStream += count
              logger.info(s"processed $count (stream) lines")
            })
            // now do the actual processing
            val mainStream = parseJson(streamData)
            process(mainStream)
            // start stream processing
            synchronized {
              if (userStoppedProcessing) {
                logger.info("processing was stopped by user during stream setup, " +
                  "not starting")
                setState(Finished)
              } else {
                logger.info("starting stream processing")
                streamStartTime = System.currentTimeMillis()
                setPhase(StreamPhase)
                ssc_.start()
              }
            }
          case Nil =>
            logger.info("not starting stream processing " +
              "(no stream source given)")
            setPhase(StopPhase)
            setState(Finished)
          case _ =>
            logger.error("not starting stream processing " +
              "(multiple streams not implemented)")
            setPhase(StopPhase)
            setState(Finished)
        }
      } else if (staticProcessingComplete && userStoppedProcessing) {
        logger.info("static processing was stopped by user, " +
          "not setting up stream")
        setPhase(StopPhase)
        setState(Finished)
      } else {
        logger.warn("static processing did not complete successfully, " +
          "not setting up stream")
        setPhase(StopPhase)
        setState(Finished)
      }
      logger.debug("bye from thread to start stream processing")
    } onFailure {
      case error: Throwable =>
        logger.error("Error while setting up stream processing", error)
        setPhase(StopPhase)
        setState(Finished)
    }

    // return a function to stop the data processing
    (() => {
      logger.info("got shutdown request from user")
      synchronized {
        userStoppedProcessing = true
      }
      logger.debug("now stopping the StreamingContext")
      currentStreamingContext.stop(stopSparkContext = false, stopGracefully = true)
      logger.debug("done stopping the StreamingContext")
      // if stream processing was not started or there was a runtime already
      // computed, we don't update the runtime
      if (streamStartTime > 0 && streamRunTime == 0) {
        streamRunTime = System.currentTimeMillis() - streamStartTime
      }
      logger.info(("processed %s items in %s ms (static) and %s items in " +
        "%s ms (stream)").format(storageCount.value, staticRunTime,
          streamCount.value, streamRunTime))
      setPhase(StopPhase)
      setState(Finished)
      (ProcessingInformation(storageCount.value, staticRunTime, maxStaticId.value),
        ProcessingInformation(streamCount.value, streamRunTime, maxStreamId.value))
    }, () => maxStaticId.value)
  }


  /**
   * Allows the user to wait for termination of the processing.
   * If an exception happens during processing, an exception will be thrown here.
   */
  def awaitTermination() = {
    logger.debug("user is waiting for termination ...")
    try {
      ssc_.awaitTermination()
      setState(Finished)
    } catch {
      case e: Throwable =>
        logger.warn("StreamingContext threw an exception (\"%s\"), shutting down".format(
          e.getMessage))
        // when we got an exception, clean up properly
        ssc_.stop(stopSparkContext = false, stopGracefully = true)
        setState(Finished)
        logger.info(s"streaming context was stopped after exception")
        throw e
    }
  }

  def getStatus(): LinkedHashMap[String, Any] = {
    var storageMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    storageMap.put("path", storageLocation)
    storageMap.put("storage_start", staticStartTime)
    storageMap.put("storage_count", storageCount.value)

    var streamMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    streamMap.put("path", streamLocations)
    streamMap.put("stream_start", streamStartTime)
    streamMap.put("stream_count", streamCount.value)

    var stsMap: LinkedHashMap[String, Any] = new LinkedHashMap()
    stsMap.put("state", _state.toString())
    stsMap.put("process_phase", _phase.getPhaseName())

    val timestamp = latestStreamTimestamp.value match {
      case Some(value) => value
      case None =>
        latestStaticTimestamp.value match {
          case Some(value) => value
          case None => ""
        }
    }

    stsMap.put("process_timestamp", timestamp)
    stsMap.put("storage", storageMap)
    stsMap.put("stream", streamMap)
    stsMap
  }
}
