package filodb.query.exec

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query.{BinaryOperator, InstantFunctionId, QueryConfig}
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn.InstantFunction
import filodb.query.exec.rangefn.LabelFunction

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

  val instantFunction = InstantFunction(function, funcParams)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    source.map { rv =>
      val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {

        private val rows = rv.rows
        private val result = new TransientRow()

        override def hasNext: Boolean = rows.hasNext

        override def next(): RowReader = {
          val next = rows.next()
          val newValue = instantFunction(next.getDouble(1))
          result.setValues(next.getLong(0), newValue)
          result
        }
      }
      IteratorBackedRangeVector(rv.key, resultIterator)
    }
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

final case class LabelFunctionMapper(function: InstantFunctionId,
                                     funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  val labelFunction = LabelFunction(function, funcParams)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    source.map { rv =>
      val newLabel = labelFunction(rv.key)
      IteratorBackedRangeVector(newLabel, rv.rows)
    }
  }
}