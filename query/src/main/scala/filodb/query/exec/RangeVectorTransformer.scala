package filodb.query.exec

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query._
import filodb.query.{BinaryOperator, InstantFunctionId, MiscellaneousFunctionId, QueryConfig, SortFunctionId}
import filodb.query.InstantFunctionId.HistogramQuantile
import filodb.query.MiscellaneousFunctionId.{LabelJoin, LabelReplace}
import filodb.query.ScalarFunctionId.Scalar
import filodb.query.SortFunctionId.{Sort, SortDesc}
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn._

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

  def funcParams: Seq[FuncArgs]
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Observable[ScalarRangeVector]): Observable[RangeVector]

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
                                             funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function"

  def evaluate(source: Observable[RangeVector], scalarRangeVector: Seq[ScalarRangeVector], queryConfig: QueryConfig,
               limit: Int, sourceSchema: ResultSchema) : Observable[RangeVector] = {
    RangeVectorTransformer.valueColumnType(sourceSchema) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function)
        if (instantFunction.isHToDoubleFunc) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new H2DoubleInstantFuncIterator(rv.rows, instantFunction.asHToDouble,
              scalarRangeVector))
          }
        } else if (instantFunction.isHistDoubleToDoubleFunc && sourceSchema.isHistDouble) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new HD2DoubleInstantFuncIterator(rv.rows, instantFunction.asHDToDouble,
              scalarRangeVector))
          }
        } else {
          throw new UnsupportedOperationException(s"Sorry, function $function is not supported right now")
        }
      case ColumnType.DoubleColumn =>
        if (function == HistogramQuantile) {
          // Special mapper to pull all buckets together from different Prom-schema time series
          val mapper = HistogramQuantileMapper(funcParams)
          mapper.apply(source, queryConfig, limit, sourceSchema, Observable.empty)
        } else {
          val instantFunction = InstantFunction.double(function)
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new DoubleInstantFuncIterator(rv.rows, instantFunction,
              scalarRangeVector))
          }
        }
      case cType: ColumnType =>
        throw new UnsupportedOperationException(s"Column type $cType is not supported for instant functions")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Observable[ScalarRangeVector]): Observable[RangeVector] = {
    if (funcParams.isEmpty) {
      evaluate(source, Nil, queryConfig, limit, sourceSchema)
    } else {
      funcParams.head match {
        case s: StaticFuncArgs   => evaluate(source, funcParams.map(x => x.asInstanceOf[StaticFuncArgs]).
                                      map(x => ScalarFixedDouble(x.timeStepParams, x.scalar)), queryConfig, limit,
                                      sourceSchema)
        case t: TimeFuncArgs     => evaluate(source, funcParams.map(x => x.asInstanceOf[TimeFuncArgs]).
                                      map(x => TimeScalar(x.timeStepParams)), queryConfig, limit, sourceSchema)
        case e: ExecPlanFuncArgs => paramResponse.map { param =>
                                      evaluate(source, Seq(param), queryConfig, limit, sourceSchema)
                                    }.flatten
        case _                   => throw new IllegalArgumentException(s"Invalid function param")
      }
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // if source is histogram, determine what output column type is
    // otherwise pass along the source
    RangeVectorTransformer.valueColumnType(source) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function)
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
                                        instantFunction: DoubleInstantFunction,
                                        scalar: Seq[ScalarRangeVector],
                                        result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val nextVal = next.getDouble(1)
    val timestamp = next.getLong(0)
    val newValue = instantFunction(nextVal, scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

private class H2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                          instantFunction: HistToDoubleIFunction,
                                          scalar: Seq[ScalarRangeVector],
                                          result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val timestamp = next.getLong(0)
    val newValue = instantFunction(next.getHistogram(1), scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

private class HD2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                           instantFunction: HDToDoubleIFunction,
                                           scalar: Seq[ScalarRangeVector],
                                           result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val timestamp = next.getLong(0)
    val newValue = instantFunction(next.getHistogram(1),
      next.getDouble(2), scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

/**
  * Applies a binary operation involving a scalar to every instant/row of the
  * range vectors
  */
final case class ScalarOperationMapper(operator: BinaryOperator,
                                       scalarOnLhs: Boolean,
                                       funcParams: Seq[FuncArgs]) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"operator=$operator, scalarOnLhs=$scalarOnLhs"

  val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Observable[ScalarRangeVector] = Observable.empty) :
  Observable[RangeVector] = {
    funcParams.head match {
    case s: StaticFuncArgs   => evaluate(source, ScalarFixedDouble(s.timeStepParams, s.scalar))
    case t: TimeFuncArgs     => evaluate(source, TimeScalar(t.timeStepParams))
    case e: ExecPlanFuncArgs => paramResponse.map(param => evaluate(source, param)).flatten
   }
  }

  private def evaluate(source: Observable[RangeVector], scalarRangeVector: ScalarRangeVector) = {
    source.map { rv =>
      val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {
        private val rows = rv.rows
        private val result = new TransientRow()

        override def hasNext: Boolean = rows.hasNext

        override def next(): RowReader = {
          val next = rows.next()
          val nextVal = next.getDouble(1)
          val timestamp = next.getLong(0)
          val sclrVal = scalarRangeVector.getValue(timestamp)
          val newValue = if (scalarOnLhs) operatorFunction.calculate(sclrVal, nextVal)
                         else operatorFunction.calculate(nextVal, sclrVal)
          result.setValues(timestamp, newValue)
          result
        }
      }
      IteratorBackedRangeVector(rv.key, resultIterator)
    }
  }
}

final case class MiscellaneousFunctionMapper(function: MiscellaneousFunctionId, funcStringParam: Seq[String] = Nil,
                                             funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams funcStringParam=$funcStringParam"

  val miscFunction: MiscellaneousFunction = {
    function match {
      case LabelReplace => LabelReplaceFunction(funcStringParam)
      case LabelJoin    => LabelJoinFunction(funcStringParam)
      case _            => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Observable[ScalarRangeVector] = Observable.empty): Observable[RangeVector] = {
    miscFunction.execute(source)
  }
}

final case class SortFunctionMapper(function: SortFunctionId) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Observable[ScalarRangeVector] = Observable.empty): Observable[RangeVector] = {
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
  override def funcParams: Seq[FuncArgs] = Nil
}

final case class ScalarFunctionMapper(function: ScalarFunctionId,
                                      timeStepParams: RangeParams) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  def scalarImpl(source: Observable[RangeVector]): Observable[RangeVector] = {

    val resultRv = source.toListL.map { rvs =>
      if (rvs.size > 1) {
        Seq(ScalarFixedDouble(timeStepParams, Double.NaN))
      } else {
        Seq(ScalarVaryingDouble(rvs.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toMap))
      }
    }.map(Observable.fromIterable)
    Observable.fromTask(resultRv).flatten
  }
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Observable[ScalarRangeVector] = Observable.empty): Observable[RangeVector] = {

    function match {
      case Scalar => scalarImpl(source)
      case _ => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
  override def funcParams: Seq[FuncArgs] = Nil

}

final case class VectorFunctionMapper() extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Observable[ScalarRangeVector] = Observable.empty): Observable[RangeVector] = {
    source.map { rv =>
      new RangeVector {
        override def key: RangeVectorKey = rv.key

        override def rows: Iterator[RowReader] = rv.rows
      }
    }
  }
  override def funcParams: Seq[FuncArgs] = Nil
}
