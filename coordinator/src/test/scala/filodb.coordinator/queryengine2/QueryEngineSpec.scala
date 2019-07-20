package filodb.coordinator.queryengine2

import akka.actor.ActorSystem
import akka.japi.Option.Some
import akka.testkit.TestProbe
import org.scalatest.{FunSpec, Matchers}
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands._
import filodb.coordinator.queryengine.{FailureProvider, FailureTimeRange, QueryRoutingPlanner, TimeRange}
import filodb.core.{DatasetRef, MetricsTestData, SpreadChange}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query
import filodb.query._
import filodb.query.exec._
import monix.eval.Task

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class QueryEngineSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset

  val dummyFailureProvider = new FailureProvider {
    override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
      Seq[FailureTimeRange]()
    }
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)(implicit sched: ExecutionContext,
                                          timeout: FiniteDuration): Task[query.QueryResponse] = ???
  }

  val engine = new QueryEngine(dataset, mapperRef, dummyFailureProvider, dummyDispatcher)

  /*
  This is the PromQL

  sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
   /
  sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
  */

  val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
               ColumnFilter("job", Filter.Equals("myService")),
               ColumnFilter("le", Filter.Equals("0.3")))

  val to = System.currentTimeMillis()
  val from = to - 50000

  val intervalSelector = IntervalSelector(from, to)

  val raw1 = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
  val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

  val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
    ColumnFilter("job", Filter.Equals("myService")))
  val raw2 = RawSeries(rangeSelector = intervalSelector, filters= f2, columns = Seq("value"))
  val windowed2 = PeriodicSeriesWithWindowing(raw2, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, Seq("job"))

  it ("should generate ExecPlan for LogicalPlan") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions())

    /*
    Following ExecPlan should be generated:

    BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=2, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#342951049])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=3, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=0, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-238515561])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=1, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    */

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      // Now there should be single level of reduce because we have 2 shards
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 2
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
        l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      }
    }
  }

  it ("should parallelize aggregation") {
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan,
      QueryOptions(Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000) )
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true

    // Now there should be multiple levels of reduce because we have 16 shards
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[ReduceAggregateExec] shouldEqual true
        l2.children.foreach { l3 =>
          l3.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
          l3.rangeVectorTransformers.size shouldEqual 2
          l3.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
          l3.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        }
      }
    }
  }

  it("should throw BadQuery if illegal column name in LogicalPlan") {
    val raw3 = raw2.copy(columns = Seq("foo"))
    intercept[BadQueryException] {
      engine.materialize(raw3, QueryOptions())
    }
  }

  it("should rename Prom __name__ filters if dataset has different metric column") {
    // Custom QueryEngine with different dataset with different metric name
    val dataset2 = dataset.copy(options = dataset.options.copy(
                     metricColumn = "kpi", shardKeyColumns = Seq("kpi", "job")))
    val engine2 = new QueryEngine(dataset2, mapperRef, dummyFailureProvider, dummyDispatcher)

    // materialized exec plan
    val execPlan = engine2.materialize(raw2, QueryOptions())
    execPlan.isInstanceOf[DistConcatExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
      val rpExec = l1.asInstanceOf[SelectRawPartitionsExec]
      rpExec.filters.map(_.column).toSet shouldEqual Set("kpi", "job")
    }
  }

  it("should use spread function to change/override spread and generate ExecPlan with appropriate shards") {
    var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
    filodbSpreadMap.put(collection.Map(("job" -> "myService")), 2)

    val spreadFunc = QueryOptions.simpleMapSpreadFunc("job", filodbSpreadMap, 1)

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions(Some(FunctionalSpreadProvider(spreadFunc)), 1000000))
    execPlan.printTree()

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children.foreach { reduceAggPlan =>
      reduceAggPlan.isInstanceOf[ReduceAggregateExec] shouldEqual true
      reduceAggPlan.children should have length (4)   // spread=2 means 4 shards
    }
  }

  it("should stitch results when spread changes during query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2)) // spread change time is in ms
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should not stitch results when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.rangeVectorTransformers.isEmpty shouldEqual true
  }

  it("should stitch results before binary join when spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
                               TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.size shouldEqual 2
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual true)
  }

  it("should not stitch results before binary join when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] should not equal true)
  }

  it ("test ") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(summed1, QueryOptions())

    execPlan.printTree()
    /*
    Following ExecPlan should be generated:

    BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=2, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#342951049])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=3, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=0, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-238515561])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=1, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    */

//    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
//    execPlan.children.foreach { l1 =>
//      // Now there should be single level of reduce because we have 2 shards
//      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
//      l1.children.foreach { l2 =>
//        l2.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
//        l2.rangeVectorTransformers.size shouldEqual 2
//        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
//        l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
//      }
//    }
  }

  it ("should generate RemoteExec when failures are present in local") {


    val to = 10000
    val from = 100
    val raw = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))


    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1500, 5000), None), FailureTimeRange("remote", datasetRef,
          TimeRange(100, 200), None), FailureTimeRange("local", datasetRef,
          TimeRange(1000, 2000), Some(dummyDispatcher)), FailureTimeRange("remote", datasetRef,
          TimeRange(100, 700), None))
      }
    }

    val engine = new QueryEngine(dataset, mapperRef, failureProvider, dummyDispatcher)
    val execPlan = engine.materialize(summed, QueryOptions())

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual(true)

    //Should be broken into local exec plan from 100 to 1000 and remote exec from 1000 to 10000
    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual(2)
    stitchRvsExec.children(0).isInstanceOf[ReduceAggregateExec] shouldEqual(true)
    stitchRvsExec.children(1).isInstanceOf[RemoteExec] shouldEqual(true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[ReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[RemoteExec]

    child1.children.length shouldEqual(2) //default spread is 1 so 2 shards

    child1.children.foreach { l1 =>
      l1.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (100)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (1000)
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }


    println("remote logical plan" + child2.params.logicalPlan.toString )
    println("original logical plan:" + summed.toString)
    println("Childeren 0")

    QueryRoutingPlanner.updateTimeLogicalPlan(summed, TimeRange(1000, 10000)).toString() shouldEqual(child2.params.logicalPlan.toString)
    child2.params.queryOptions.spreadProvider.isDefined shouldEqual(false) // QueryOptions should be false

    println("ExecPlan :")
    execPlan.printTree()
    println("execPlan seq:" + execPlan.getPlan())
    execPlan.getPlan().foreach(println(_))

  }

}
