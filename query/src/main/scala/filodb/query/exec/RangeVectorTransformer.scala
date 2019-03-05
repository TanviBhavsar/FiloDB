package filodb.query.exec

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query.{BinaryOperator, InstantFunctionId, MiscellaneousFunctionId, QueryConfig}
import filodb.query.InstantFunctionId.HistogramQuantile
import filodb.query.MiscellaneousFunctionId.LabelReplace
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn.{
  DoubleInstantFunction, HistToDoubleIFunction,
  InstantFunction, LabelReplaceFunction, MiscellaneousFunction
}



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
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector]

  /**
    * Default implementation retains source schema
    */
  def schema(dataset: Dataset, source: ResultSchema): ResultSchema = source

  /**
    * Args to use for the RangeVectorTransformer for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  protected[exec] def args: String
}

object RangeVectorTransformer {
  def valueColumnType(schema: ResultSchema): ColumnType = {
    require(schema.isTimeSeries, "Cannot return periodic data from a dataset that is not time series based")
    require(schema.columns.size == 2, "Cannot return periodic data from a dataset that is not time series based")
    schema.columns(1).colType
  }
}

/**
  * Applies an instant vector function to every instant/row of the
  * range vectors
  */
final case class InstantVectorFunctionMapper(function: InstantFunctionId,
                                             funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    RangeVectorTransformer.valueColumnType(sourceSchema) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new H2DoubleInstantFuncIterator(rv.rows, instantFunction.asHToDouble))
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
          val instantFunction = InstantFunction.double(function, funcParams)
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new DoubleInstantFuncIterator(rv.rows, instantFunction))
          }
        }
      case cType: ColumnType =>
        throw new UnsupportedOperationException(s"Column type $cType is not supported for instant functions")
    }
  }

  override def schema(dataset: Dataset, source: ResultSchema): ResultSchema = {
    // if source is histogram, determine what output column type is
    // otherwise pass along the source
    RangeVectorTransformer.valueColumnType(source) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc) {
          // Hist to Double function, so output schema is double
          source.copy(columns = Seq(source.columns.head,
                                    source.columns(1).copy(colType = ColumnType.DoubleColumn)))
        } else { source }
      case cType: ColumnType          =>
        source
    }
  }
}

private class DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                        instantFunction: DoubleInstantFunction,
                                        result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
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

/**
  * Applies a binary operation involving a scalar to every instant/row of the
  * range vectors
  */
final case class ScalarOperationMapper(operator: BinaryOperator,
                                       scalar: AnyVal,
                                       scalarOnLhs: Boolean) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"operator=$operator, scalar=$scalar"

  val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    source.map { rv =>
      val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {

        private val rows = rv.rows
        private val result = new TransientRow()
        private val sclrVal = scalar.asInstanceOf[Double]

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
  }

  // TODO all operation defs go here and get invoked from mapRangeVector
}

final case class MiscellaneousFunctionMapper(function: MiscellaneousFunctionId,
                                             funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    val miscFunction: MiscellaneousFunction = {
      function match {
        case LabelReplace => LabelReplaceFunction(source, funcParams)
        case _ => throw new UnsupportedOperationException(s"$function not supported.")
      }
    }
    if (miscFunction.validator())
      return miscFunction.execute()
    return source
  }
}