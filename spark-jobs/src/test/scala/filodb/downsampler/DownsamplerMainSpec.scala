package filodb.downsampler

import java.io.{BufferedWriter, File, FileWriter}
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.util

import filodb.core.GlobalConfig.systemConfig
import org.json4s.jackson.Serialization

import scala.io.Source
//import java.math.BigInteger
import java.time.Instant

import com.typesafe.config.{ConfigException, ConfigFactory}
import filodb.cardbuster.CardinalityBuster
import filodb.core.GlobalScheduler._
import filodb.core.MachineMetricsData
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder, RecordSchema}
import filodb.core.downsample.{DownsampledTimeSeriesStore, OffHeapMemory}
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.Filter.Equals
import filodb.core.query._
import filodb.core.store.{AllChunkScan, PartKeyRecord, SinglePartitionScan, StoreConfig}
import filodb.downsampler.chunk.{BatchDownsampler, Downsampler, DownsamplerSettings}
import filodb.downsampler.index.{DSIndexJobSettings, IndexJobDriver}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}
import filodb.memory.format.{PrimitiveVectorReader, UnsafeUtils}
import filodb.query.QueryResult
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.spark.{SparkConf, SparkException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val conf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))

  val settings = new DownsamplerSettings(conf)
  val queryConfig = new QueryConfig(settings.filodbConfig.getConfig("query"))
  val dsIndexJobSettings = new DSIndexJobSettings(settings)
  val batchDownsampler = new BatchDownsampler(settings)

  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)

  val rawColStore = batchDownsampler.rawCassandraColStore
  val downsampleColStore = batchDownsampler.downsampleCassandraColStore

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                  |ingestion-buffer-mem-size = 30MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
                                     Map.empty, 100, rawDataStoreConfig)

  val untypedName = "my_untyped"
  var untypedPartKeyBytes: Array[Byte] = _

  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _

  val counterName = "my_counter"
  var counterPartKeyBytes: Array[Byte] = _

  val histName = "my_histogram"
  var histPartKeyBytes: Array[Byte] = _

  val gaugeLowFreqName = "my_gauge_low_freq"
  var gaugeLowFreqPartKeyBytes: Array[Byte] = _

  val lastSampleTime = 74373042000L
  val pkUpdateHour = hour(lastSampleTime)

  val metricNames = Seq(gaugeName, gaugeLowFreqName, counterName, histName, untypedName)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

  override def beforeAll(): Unit = {
    batchDownsampler.downsampleRefsByRes.values.foreach { ds =>
      downsampleColStore.initialize(ds, 4).futureValue
      downsampleColStore.truncate(ds, 4).futureValue
    }
    rawColStore.initialize(batchDownsampler.rawDatasetRef, 4).futureValue
    rawColStore.truncate(batchDownsampler.rawDatasetRef, 4).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }


  it("test") {
    // val brHex = "0x9e0100009808120000002e00000039ae758e1a006e6f64655f66696c6573797374656d5f73697a655f62797465737201c10a0075732d656173742d3161085f736f757263655f06006d6f73616963065f737465705f02003630c00e006163692d6b756265726e6574657308617263687479706504006b75626507636c75737465720a0075732d656173742d31610a6461746163656e74657206006d616964656e0664657669636504006e73667308656e64706f696e740c00687474702d6d6574726963730666737479706504006e736673c4130031302e3139312e3135332e3134323a39313030c711006170632d6e6f64652d6578706f727465720a6d6f756e74706f696e7437002f72756e2f6e65746e732f636e69746573742d61353330363063632d323964342d633265302d323332642d366264353838613864613337096e616d6573706163650e006170632d70726f6d657468657573046e6f64653100706f72742d2d31303236302e6b6e6f6465323730312e757375716f3238612e6b6b2e636c6f75642e6170706c652e636f6d077365727669636507006b7562656c6574"
    //val brHex = "0x2c0000000f1712000000200000004b8b36940c006d794d65747269634e616d650e00c104006d794e73c004006d795773"
    val brHex ="0xc90200009808120000001b000000937583d6070047617567653136b002c10e00436172642d316b2d4d43502d3537065f737465705f02003130c00d006163692d74656c656d65747279c4240031623563303762302d393031312d343333352d383262632d3834383562336336306532372f6d6f736169635f6d6574726f5f6163695f74656c656d657472795f636c75737465725f696e736964655f6170706c6534006d6f736169635f6d6574726f5f6163695f74656c656d657472795f636c75737465725f696e736964655f6170706c652d56616c37236d6f736169635f6d6574726f5f6461746163657465725f696e736964655f6170706c6528006d6f736169635f6d6574726f5f6461746163657465725f696e736964655f6170706c652d56616c37326d6f736169635f6d6574726f5f686f73746e616d655f6170706c655f6461746163657465725f696e736964655f6170706c6537006d6f736169635f6d6574726f5f686f73746e616d655f6170706c655f6461746163657465725f696e736964655f6170706c652d56616c372f6d6f736169635f6d6574726f5f696e7374616e63655f73686f72747461736b5f69645f696e736964655f6170706c6535006d6f736169635f6d6574726f5f696e7374616e63655f73686f72747461736b5f69645f696e736964655f6170706c652d56616c3537236d6f736169635f6d6574726f5f6f7065726174696f6e5f696e736964655f6170706c6528006d6f736169635f6d6574726f5f6f7065726174696f6e5f696e736964655f6170706c652d56616c32236d6f736169635f6d6574726f5f706172746974696f6e5f696e736964655f6170706c6528006d6f736169635f6d6574726f5f706172746974696f6e5f696e736964655f6170706c652d56616c37196d6f736169635f6d6574726f5f736572766963655f6e616d651e006d6f736169635f6d6574726f5f736572766963655f6e616d652d56616c31"
    val dirName = "/Users/tanvibhavsar/Downloads/MosaicPerf/partKeysPerf1/"

    val file = new File("decodedPartKeys2")
    val bw = new BufferedWriter(new FileWriter(file))
    new java.io.File(dirName).listFiles.foreach { filename =>
      println("filename:" +filename)
      for (brHex <- Source.fromFile(filename).getLines) {
       // println(brHex)

        val brHex2 = if (brHex.startsWith("0x")) brHex.substring(2) else brHex
        val config = systemConfig.getConfig("filodb")
        val biBytes = new BigInteger("10" + brHex2, 16).toByteArray
        val pkBytes = util.Arrays.copyOfRange(biBytes, 1, biBytes.length)
        val partSchema = Schemas.fromConfig(config).get.part
        // println("decoded value:" + partSchema.binSchema.toStringPairs(pkBytes, UnsafeUtils.arayOffset).toMap.values.toList)
        import org.json4s._
        implicit val formats = Serialization.formats(NoTypeHints)
        val decodedString = partSchema.binSchema.toStringPairs(pkBytes, UnsafeUtils.arayOffset).toMap
        val res = Serialization.write(decodedString)
        bw.write(res)
        bw.newLine()
        //println("res:" + res)
      }
    }
    bw.close()
  }

//    it("test2") {
//       val brHex = "0x9e0100009808120000002e00000039ae758e1a006e6f64655f66696c6573797374656d5f73697a655f62797465737201c10a0075732d656173742d3161085f736f757263655f06006d6f73616963065f737465705f02003630c00e006163692d6b756265726e6574657308617263687479706504006b75626507636c75737465720a0075732d656173742d31610a6461746163656e74657206006d616964656e0664657669636504006e73667308656e64706f696e740c00687474702d6d6574726963730666737479706504006e736673c4130031302e3139312e3135332e3134323a39313030c711006170632d6e6f64652d6578706f727465720a6d6f756e74706f696e7437002f72756e2f6e65746e732f636e69746573742d61353330363063632d323964342d633265302d323332642d366264353838613864613337096e616d6573706163650e006170632d70726f6d657468657573046e6f64653100706f72742d2d31303236302e6b6e6f6465323730312e757375716f3238612e6b6b2e636c6f75642e6170706c652e636f6d077365727669636507006b7562656c6574"
//      //val brHex = "0x2c0000000f1712000000200000004b8b36940c006d794d65747269634e616d650e00c104006d794e73c004006d795773"
//
//        val brHex2 = if (brHex.startsWith("0x")) brHex.substring(2) else brHex
//        val config = systemConfig.getConfig("filodb")
//        val array = new BigInteger(brHex2, 16).toByteArray
//        val schema = Schemas.fromConfig(config).get.schemas.head._2
//        val decoded = schema.partKeySchema.stringify(array)
//      //schema.partKeySchema.
//        //if (decoded.contains())
//        println("decoded value:" + decoded)
//
//      }

//    val brHex2 = if (brHex.startsWith("0x")) brHex.substring(2) else brHex
//    val array = new BigInteger(brHex2, 16).toByteArray
//    val config = systemConfig.getConfig("filodb")
//    //val schema = Schemas.fromConfig(config).get
//    val schema = Schemas.fromConfig(config).get.schemas.head._2
//    //val schema = Schemas.gauge
//    println("decoded value:" + schema.partKeySchema.stringify(array))

//   // val strPairs = schema.part.binSchema.toStringPairs(array, UnsafeUtils.arayOffset)
//
//    println("strPairs of prod data:" + strPairs)

  it ("should write untyped data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.untyped)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.untyped, untypedName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.untyped, partKey,
      0, offheapMem.bufferPools(Schemas.untyped.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    untypedPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, untypedName, seriesTags),
      Seq(74372802000L, 5d, untypedName, seriesTags),

      Seq(74372861000L, 9d, untypedName, seriesTags),
      Seq(74372862000L, 11d, untypedName, seriesTags),

      Seq(74372921000L, 13d, untypedName, seriesTags),
      Seq(74372922000L, 15d, untypedName, seriesTags),

      Seq(74372981000L, 17d, untypedName, seriesTags),
      Seq(74372982000L, 15d, untypedName, seriesTags),

      Seq(74373041000L, 13d, untypedName, seriesTags),
      Seq(74373042000L, 11d, untypedName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.untyped.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(untypedPartKeyBytes, 74372801000L, 74373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugePartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      Seq(74372861000L, 9d, gaugeName, seriesTags),
      Seq(74372862000L, 11d, gaugeName, seriesTags),

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      Seq(74372981000L, 17d, gaugeName, seriesTags),
      Seq(74372982000L, 15d, gaugeName, seriesTags),

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugePartKeyBytes, 74372801000L, 74373042000L, Some(150))

    println("gaugePartKeyBytes:" + gaugePartKeyBytes)
    println("gaugePartKeyBytes string:" + new String(gaugePartKeyBytes, StandardCharsets.UTF_8))
    val brHex = gaugePartKeyBytes
    val brHex2 = if (brHex.startsWith("0x")) brHex.slice(2, brHex.length) else brHex
    //val array = new BigInteger(brHex2, 16).toByteArray
    val config = systemConfig.getConfig("filodb")
    val schema = Schemas.fromConfig(config).get.schemas.head._2
    //val schema = Schemas.gauge

    val strPairs =  batchDownsampler.schemas.part.binSchema.toStringPairs(gaugePartKeyBytes, UnsafeUtils.arayOffset)

   println("strPairs of gaugePartKeyBytes:" + strPairs)
    println("decoded value:" + schema.partKeySchema.stringify(brHex2))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write low freq gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeLowFreqName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugeLowFreqPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      // skip next minute

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      // skip next minute

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugeLowFreqPartKeyBytes, 74372801000L, 74373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write prom counter data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promCounter)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, counterName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promCounter, partKey,
      0, offheapMem.bufferPools(Schemas.promCounter.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    counterPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, counterName, seriesTags),
      Seq(74372801500L, 4d, counterName, seriesTags),
      Seq(74372802000L, 5d, counterName, seriesTags),

      Seq(74372861000L, 9d, counterName, seriesTags),
      Seq(74372861500L, 10d, counterName, seriesTags),
      Seq(74372862000L, 11d, counterName, seriesTags),

      Seq(74372921000L, 2d, counterName, seriesTags),
      Seq(74372921500L, 7d, counterName, seriesTags),
      Seq(74372922000L, 15d, counterName, seriesTags),

      Seq(74372981000L, 17d, counterName, seriesTags),
      Seq(74372981500L, 1d, counterName, seriesTags),
      Seq(74372982000L, 15d, counterName, seriesTags),

      Seq(74373041000L, 18d, counterName, seriesTags),
      Seq(74373042000L, 20d, counterName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promCounter.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(counterPartKeyBytes, 74372801000L, 74373042000L, Some(1))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write prom histogram data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, histName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey,
      0, offheapMem.bufferPools(Schemas.promHistogram.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    histPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histName, seriesTags),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histName, seriesTags),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histName, seriesTags),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), histName, seriesTags),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histName, seriesTags),
      Seq(74372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), histName, seriesTags),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), histName, seriesTags),
      Seq(74372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), histName, seriesTags),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), histName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), histName, seriesTags),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), histName, seriesTags),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), histName, seriesTags),
      Seq(74373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), histName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(histPartKeyBytes, 74372801000L, 74373042000L, Some(199))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  val numShards = dsIndexJobSettings.numShards
  val bulkPkUpdateHours = {
    val start = pkUpdateHour / 6 * 6 // 6 is number of hours per downsample chunk
    start until start + 6
  }

  it("should simulate bulk part key records being written into raw for migration") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val schemas = Seq(Schemas.promHistogram, Schemas.gauge, Schemas.promCounter)
    case class PkToWrite(pkr: PartKeyRecord, shard: Int, updateHour: Long)
    val pks = for { i <- 0 to 10000 } yield {
      val schema = schemas(i % schemas.size)
      val partKey = partBuilder.partKeyFromObjects(schema, s"bulkmetric$i", bulkSeriesTags)
      val bytes = schema.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey)
      PkToWrite(PartKeyRecord(bytes, i, i + 500, Some(-i)), i % numShards,
        bulkPkUpdateHours(i % bulkPkUpdateHours.size))
    }

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    pks.groupBy(k => (k.shard, k.updateHour)).foreach { case ((shard, updHour), shardPks) =>
      rawColStore.writePartKeys(rawDataset.ref, shard, Observable.fromIterable(shardPks).map(_.pkr),
        259200, updHour).futureValue
    }
  }

  it ("should free all offheap memory") {
    offheapMem.free()
  }

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    val downsampler = new Downsampler(settings, batchDownsampler)
    downsampler.run(sparkConf).close()
  }

  it ("should migrate partKey data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.index.timeInPeriodOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    sparkConf.set("spark.filodb.downsampler.index.toHourExclOverride", (pkUpdateHour + 6 + 1).toString)
    val indexUpdater = new IndexJobDriver(settings, dsIndexJobSettings)
    indexUpdater.run(sparkConf).close()
  }

  it ("should verify migrated partKey data and match the downsampled schema") {

    def pkMetricSchemaReader(pkr: PartKeyRecord): (String, String) = {
      val schemaId = RecordSchema.schemaID(pkr.partKey, UnsafeUtils.arayOffset)
      val partSchema = batchDownsampler.schemas(schemaId)
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      (strPairs.find(p => p._1 == "_metric_").get._2, partSchema.data.name)
    }

    val metrics = Set((counterName, Schemas.promCounter.name),
      (gaugeName, Schemas.dsGauge.name),
      (gaugeLowFreqName, Schemas.dsGauge.name),
      (histName, Schemas.promHistogram.name))
    val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")), 0)
    val tsSchemaMetric = Await.result(partKeys.map(pkMetricSchemaReader).toListL.runAsync, 1 minutes)
    tsSchemaMetric.filter(k => metricNames.contains(k._1)).toSet shouldEqual metrics
  }

  it("should verify bulk part key record migration and validate completeness of PK migration") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }
    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
                                                     shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain untyped part key - we dont downsample untyped
    readKeys shouldEqual (0 to 10000).map(i => s"bulkmetric$i").toSet ++ (metricNames.toSet - untypedName)
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372862000L, 9.0, 11.0, 20.0, 2.0, 10.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74372982000L, 15.0, 17.0, 32.0, 2.0, 16.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  /*
  Tip: After running this spec, you can bring up the local downsample server and hit following URL on browser
  http://localhost:9080/promql/prometheus/api/v1/query_range?query=my_gauge%7B_ws_%3D%27my_ws%27%2C_ns_%3D%27my_ns%27%7D&start=74372801&end=74373042&step=10&verbose=true&spread=2
   */

  it("should read and verify low freq gauge in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugeLowFreqPartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugeLowFreqPartKeyBytes,
                                                                          batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugeLowFreqPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual dsGaugeLowFreqPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),
      (74372802000L, 5d),

      (74372862000L, 11d),

      (74372921000L, 2d),
      (74372922000L, 15d),

      (74372981000L, 17d),
      (74372981500L, 1d),
      (74372982000L, 15d),

      (74373042000L, 20d)

    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3))

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d)),

      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),

      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (74372922000L, 15d, 19d, Seq(1d, 15d, 19d)),

      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (74372982000L, 15d, 15d, Seq(0d, 15d, 15d)),

      (74373042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData2 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData2, Some(5.minutes.toMillis))

    downsampledPart2.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (74372982000L, 3.0, 17.0, 88.0, 8.0, 11.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),

      (74372862000L, 11d),

      (74372921000L, 2d),

      (74372981000L, 17d),
      (74372981500L, 1d),

      (74372982000L, 15.0d),

      (74373042000L, 20.0d)
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3))

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),
      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (74372982000L, 15.0d, 15.0d, Seq(0.0, 15.0, 15.0)),
      (74373042000L, 20.0d, 25.0d, Seq(4.0, 20.0, 25.0))
    )
  }

  it("should bring up DownsampledTimeSeriesShard and be able to read data using SelectRawPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

    Seq(gaugeName, gaugeLowFreqName, counterName, histName).foreach { metricName =>
      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(metricName))
      val exec = MultiSchemaPartitionsExec(QueryContext(plannerParams = PlannerParams(sampleLimit = 1000)),
        InProcessPlanDispatcher, batchDownsampler.rawDatasetRef, 0, queryFilters, AllChunkScan)

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 1
      res.result.foreach(_.rows.nonEmpty shouldEqual true)
    }

  }

  it("should bring up DownsampledTimeSeriesShard and NOT be able to read untyped data using SelectRawPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(untypedName))
      val exec = MultiSchemaPartitionsExec(QueryContext(plannerParams
        = PlannerParams(sampleLimit = 1000)), InProcessPlanDispatcher, batchDownsampler.rawDatasetRef, 0,
        queryFilters, AllChunkScan)

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 0
  }

  it("should bring up DownsampledTimeSeriesShard and be able to read specific columns " +
      "from gauge using MultiSchemaPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)
    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)
    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue
    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(gaugeName))
    val exec = MultiSchemaPartitionsExec(QueryContext(plannerParams = PlannerParams(sampleLimit = 1000)),
      InProcessPlanDispatcher, batchDownsampler.rawDatasetRef, 0, queryFilters, AllChunkScan, colName = Option("sum"))
    val querySession = QuerySession(QueryContext(), queryConfig)
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
      .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
    queryScheduler.shutdown()
    res.result.size shouldEqual 1
    res.result.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual
      List((74372982000L, 88.0), (74373042000L, 24.0))
  }

  it ("should fail when cardinality buster is not configured with any delete filters") {

    // This test case is important to ensure that a run with missing configuration will not do unintended deletes
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val settings2 = new DownsamplerSettings(conf)
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    val caught = intercept[SparkException] {
      cardBuster.run(sparkConf).close()
    }
    caught.getCause.asInstanceOf[ConfigException.Missing].getMessage
      .contains("No configuration setting found for key 'cardbuster'") shouldEqual true
  }

  it ("should be able to bust cardinality by time filter in downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString(
      s"""
         |filodb.cardbuster.delete-pk-filters = [
         | {
         |    _ns_ = "bulk_ns"
         |    _ws_ = "bulk_ws"
         | }
         |]
         |filodb.cardbuster.delete-startTimeGTE = "${Instant.ofEpochMilli(0).toString}"
         |filodb.cardbuster.delete-endTimeLTE = "${Instant.ofEpochMilli(600).toString}"
         |""".stripMargin)

    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    cardBuster.run(sparkConf).close()

    sparkConf.set("spark.filodb.cardbuster.inDownsampleTables", "true")
    cardBuster.run(sparkConf).close()
  }

  it("should verify bulk part key records are absent after card busting by time filter in downsample tables") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }

    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // downsample set should not have a few bulk metrics
    readKeys.size shouldEqual 9904

    val readKeys2 = (0 until 4).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // raw set should remain same since inDownsampleTables=true in
    readKeys2.size shouldEqual 10006
  }

  it ("should be able to bust cardinality in both raw and downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString( """
                                                          |filodb.cardbuster.delete-pk-filters = [
                                                          | {
                                                          |    _ns_ = "bulk_ns"
                                                          |    _ws_ = "bulk_ws"
                                                          | }
                                                          |]
                                                        """.stripMargin)
    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)

    // first run for downsample tables
    cardBuster.run(sparkConf).close()

    // then run for raw tables
    sparkConf.set("spark.filodb.cardbuster.inDownsampleTables", "false")
    cardBuster.run(sparkConf).close()
  }

  it("should verify bulk part key records are absent after deletion in both raw and downsample tables") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }

    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys shouldEqual (metricNames.toSet - untypedName)

    val readKeys2 = (0 until 4).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys2 shouldEqual metricNames.toSet
  }


}
