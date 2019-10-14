package filodb.query.exec

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query.ScalarFunctionId.Scalar
import filodb.query.{BinaryOperator, InstantFunctionId, MiscellaneousFunctionId, QueryConfig, SortFunctionId}
import filodb.query.InstantFunctionId.HistogramQuantile
import filodb.query.MiscellaneousFunctionId.{LabelJoin, LabelReplace}
import filodb.query.SortFunctionId.{Sort, SortDesc}
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn._
import filodb.query._
import monix.reactive.Observable

private case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

/**
  * Implementations can provide ways to transform RangeVector
  * results generated by ExecPlan nodes.
  *
  * Reason why these are not ExecPlan nodes themselves is because
  * they can be applied on the same node where the base RangeVectors
  * are sourced.
  *
  * It can safely be assumed that the operations in these nodes are
  * compute intensive and not I/O intensive.
  */
trait RangeVectorTransformer extends java.io.Serializable {

  def funcParams: Seq[ExecPlan]  // to do validate that it return scalar range
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Option[Observable[Seq[RangeVector]]] = None): Observable[RangeVector]

  /**
    * Default implementation retains source schema
    */
  def schema(source: ResultSchema): ResultSchema = source

  /**
    * Args to use for the RangeVectorTransformer for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  protected[exec] def args: String
}

object RangeVectorTransformer {
  def valueColumnType(schema: ResultSchema): ColumnType = {
    require(schema.isTimeSeries, s"Schema $schema is not time series based, cannot continue query")
    require(schema.columns.size >= 2, s"Schema $schema has less than 2 columns, cannot continue query")
    schema.columns(1).colType
  }
}

/**
  * Applies an instant vector function to every instant/row of the
  * range vectors
  */
final case class InstantVectorFunctionMapper(function: InstantFunctionId,
                                             funcParams: Seq[ExecPlan] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Option[Observable[Seq[RangeVector]]] = None): Observable[RangeVector] = {

    val funcParamScalar = funcParams match {
      case d: ScalarFixedDouble => d
      case t: ScalarVector => t
      //case h: HourScalar   => h
      case _ => throw new BadQueryException("Invalid logical plan")
    }

    RangeVectorTransformer.valueColumnType(sourceSchema) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new H2DoubleInstantFuncIterator(rv.rows, instantFunction.asHToDouble))
          }
        } else if (instantFunction.isHistDoubleToDoubleFunc && sourceSchema.isHistDouble) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new HD2DoubleInstantFuncIterator(rv.rows, instantFunction.asHDToDouble))
          }
        } else {
          throw new UnsupportedOperationException(s"Sorry, function $function is not supported right now")
        }
      case ColumnType.DoubleColumn =>
        if (function == HistogramQuantile) {
          // Special mapper to pull all buckets together from different Prom-schema time series
          val mapper = HistogramQuantileMapper(funcParams)
          mapper.apply(source, queryConfig, limit, sourceSchema)
        } else {
          //val instantFunction = InstantFunction.double(function, funcParams)
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new DoubleInstantFuncIterator(rv.rows, function, funcParamScalar))
          }
        }
      case cType: ColumnType =>
        throw new UnsupportedOperationException(s"Column type $cType is not supported for instant functions")
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // if source is histogram, determine what output column type is
    // otherwise pass along the source
    RangeVectorTransformer.valueColumnType(source) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc || instantFunction.isHistDoubleToDoubleFunc) {
          // Hist to Double function, so output schema is double
          source.copy(columns = Seq(source.columns.head, ColumnInfo("value", ColumnType.DoubleColumn)))
        } else { source }
      case cType: ColumnType          =>
        source
    }
  }
}

private class DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                        function: InstantFunctionId,
                                        param: ScalarVector,
                                        result: TransientRow = new TransientRow()) extends Iterator[RowReader] {

//  def apply(rows: Iterator[RowReader],
//            instantFunction: DoubleInstantFunction,
//            param: ScalarVector,
//            result: TransientRow = new TransientRow()): DoubleInstantFuncIterator = new DoubleInstantFuncIterator(rows, instantFunction, param, result)
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val instantFunction = InstantFunction.double(function, Seq(param.rows.next().getDouble(1)))
    val newValue = instantFunction(next.getDouble(1))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

private class H2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                          instantFunction: HistToDoubleIFunction,
                                          result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val newValue = instantFunction(next.getHistogram(1))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

private class HD2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                           instantFunction: HDToDoubleIFunction,
                                           result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val newValue = instantFunction(next.getHistogram(1), next.getDouble(2))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

/**
  * Applies a binary operation involving a scalar to every instant/row of the
  * range vectors
  */
final case class ScalarOperationMapper(operator: BinaryOperator,
                                       scalarOnLhs: Boolean,
                                       funcParams: Seq[ExecPlan]) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"operator=$operator, scalar=$funcParams"

  val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Option[Observable[Seq[RangeVector]]] = None) : Observable[RangeVector] = {

    //    val paramsList = new mutable.ArrayBuffer[RowReader]()
    //    paramRangeVector.foreach(x => paramsList ++ x.head.asInstanceOf[ScalarVaryingDouble].getRowsList)
    if (paramResponse.isEmpty) {
      source.map { rv =>
        val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {

          private val rows = rv.rows
          private val result = new TransientRow()
          private val sclrVal = funcParams.head.asInstanceOf[ScalarFixedDoubleParamExec].value
          // to DO sclVal for time function

          override def hasNext: Boolean = rows.hasNext

          override def next(): RowReader = {
            val next = rows.next()
            val nextVal = next.getDouble(1)
            val newValue = if (scalarOnLhs) operatorFunction.calculate(sclrVal, nextVal)
            else  operatorFunction.calculate(nextVal, sclrVal)
            result.setValues(next.getLong(0), newValue)
            result
          }
        }
        IteratorBackedRangeVector(rv.key, resultIterator)
      }

    } else {
      paramResponse.get.map { param =>
        println("rows size:" + param.head.asInstanceOf[SerializableRangeVector].rows.size)
        val rowMap = param.head.asInstanceOf[SerializableRangeVector].getRowMap
        source.map { rv =>
          val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {
            var paramIndex = 0
            private val rows = rv.rows
            private val result = new TransientRow()

            override def hasNext: Boolean = rows.hasNext

            override def next(): RowReader = {
              val next = rows.next()
              val nextVal = next.getDouble(1)
              val timestamp = next.getLong(0)
              println("rowMap:" + rowMap)
              val sclrVal = rowMap.get(timestamp).get
              val newValue = if (scalarOnLhs) operatorFunction.calculate(sclrVal, nextVal)
              else operatorFunction.calculate(nextVal, sclrVal)
              result.setValues(timestamp, newValue)
              paramIndex += 1
              result
            }
          }
          IteratorBackedRangeVector(rv.key, resultIterator)
        }
      }.flatten

    }
  }
  // TODO all operation defs go here and get invoked from mapRangeVector
  //override def = Nil
}

final case class MiscellaneousFunctionMapper(function: MiscellaneousFunctionId, funcParams: Seq[ExecPlan] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"
  val stringParam : Seq[StringParamExec] = if (funcParams.forall(p => p.isInstanceOf[StringParamExec])){
    funcParams.map(_.asInstanceOf[StringParamExec])
  } else {
    Nil
  }
//  val stringParam : Seq[StringParamExec] = funcParams match {
//    case str: Seq[StringParamExec] => str
//    case _ => throw new UnsupportedOperationException(s"$funcParams not supported.")
//  }

  val miscFunction: MiscellaneousFunction = {
    function match {
      case LabelReplace => LabelReplaceFunction(stringParam)
      case LabelJoin => LabelJoinFunction(stringParam)
      case _ => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Option[Observable[Seq[RangeVector]]] = None): Observable[RangeVector] = {
    miscFunction.execute(source)
  }
}

final case class SortFunctionMapper(function: SortFunctionId) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Option[Observable[Seq[RangeVector]]] = None): Observable[RangeVector] = {
    if (sourceSchema.columns(1).colType == ColumnType.DoubleColumn) {

      val ordering: Ordering[Double] = function match {
        case Sort => (Ordering[Double])
        case SortDesc => (Ordering[Double]).reverse
        case _ => throw new UnsupportedOperationException(s"$function not supported.")
      }

      val resultRv = source.toListL.map { rvs =>
        rvs.map { rv =>
          new RangeVector {
            override def key: RangeVectorKey = rv.key

            override def rows: Iterator[RowReader] = new BufferableIterator(rv.rows).buffered
          }
        }.sortBy { rv => rv.rows.asInstanceOf[BufferedIterator[RowReader]].head.getDouble(1)
        }(ordering)

      }.map(Observable.fromIterable)

      Observable.fromTask(resultRv).flatten
    } else {
      source
    }
  }
  override def funcParams: Seq[ExecPlan] = Nil
}

final case class ScalarFunctionMapper(function: ScalarFunctionId,
                                      timeStepParams: TimeStepParams) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val recSchema = SerializableRangeVector.toSchema(columns)

  val resultSchema = ResultSchema(columns, 1)
 // val resultSchema = da
  val builder = SerializableRangeVector.newBuilder()

  //  val scalarFunction: ScalarFunction = {
  //    function match {
  //      case LabelReplace => LabelReplaceFunction(funcParams)
  //      case LabelJoin =>  LabelJoinFunction(funcParams)
  //      case _ => throw new UnsupportedOperationException(s"$function not supported.")
  //    }
  //  }

  def scalarImpl(source: Observable[RangeVector]): Observable[RangeVector] = {

//    val paramsList = paramsResponse.get().map {
//      case (QueryResult(_, _, result), i) => (result)
//      case (QueryError(_, ex), _)         => throw ex
//    }.toListL





    val resultRv = source.toListL.map { rvs =>
      println("rvs size in scalar function:" + rvs.size)
      if (rvs.size >1)
        Seq(ScalarFixedDouble(timeStepParams.start, timeStepParams.end, timeStepParams.step, Double.NaN))
       else
     Seq(ScalarVaryingDouble(rvs.head.rows))
    }.map(Observable.fromIterable)
    Observable.fromTask(resultRv).flatten

  }
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Option[Observable[Seq[RangeVector]]] = None): Observable[RangeVector] = {

//    val taskOfResults = paramsResponse.get.map {
//      case (QueryResult(_, _, result)) => result
//      case (QueryError(_, ex))         => throw ex
//    }.toListL
    function match {
      case Scalar => scalarImpl(source)
      //case Time => timeImpl(source)
      case _ => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
  override def funcParams: Seq[ExecPlan] = Nil

}

