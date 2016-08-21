//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package commbank.coppersmith.scalding

import com.twitter.scalding.{Execution, TupleSetter, TypedPipe}

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api._

import commbank.coppersmith._, Feature._
import Partitions.PathComponents
import FeatureSink.AttemptedWriteToCommitted
import CoppersmithStats.fromTypedPipe

/**
  * Parquet FeatureSink implementation - create using HiveParquetSink.apply in companion object.
  */
case class HiveParquetSink[T <: ThriftStruct : Manifest : FeatureValueEnc, P : TupleSetter] private(
  table:         HiveTable[T, (P, T)],
  partitionPath: Path
) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)]): FeatureSink.WriteResult = {
    FeatureSink.isCommitted(partitionPath).flatMap(committed =>
      if (committed) {
        Execution.from(Left(AttemptedWriteToCommitted(partitionPath)))
      } else {
        val eavts = features.map(implicitly[FeatureValueEnc[T]].encode).withCounter("write.parquet")
        table.writeExecution(eavts).flatMap(restoreCounters).map(_ => Right(Set(partitionPath)))
      }
    )
  }

  // Workaround required for correctly logging coppersmith stats: maestro's HiveTable.writeExecution
  // resets counters, then returns them; reverse this by restoring the counter values.
  import com.twitter.scalding.{ExecutionCounters, StatKey, Mode, TupleConverter}
  import com.twitter.scalding.typed.TypedPipeFactory
  import com.twitter.maple.tap.MemorySourceTap
  import com.twitter.scalding.{IterableSource, NullSource}
  import cascading.operation.{BaseOperation, Filter, FilterCall}
  import cascading.tuple.Fields
  import cascading.pipe.Each
  import cascading.flow.{FlowProcess, FlowDef}
  import com.twitter.scalding.Dsl._
  def restoreCounters(counters: ExecutionCounters): Execution[Unit] = {
    println("restoreCounters called with " + counters.toMap.toString)

    class SetCounters extends BaseOperation[Any] with Filter[Any] {
      def isRemove(flowProcess: FlowProcess[_], filterCall: FilterCall[Any]) = {
        println("isRemove")
        false
      }
    }

    Execution.from(TypedPipeFactory({ (fd, mode) =>
    println("typed pipe factory called")
    val values = counters.toMap.toList.map { case (StatKey(name, group), value) => (name, group, value) }
    val valuePipe = IterableSource(values, new Fields("name", "group", "value")).read(fd, mode)
    //val pipe = new Each(valuePipe, new SetCounters).write(NullSource)(fd, mode)
    TypedPipe.from[Unit](pipe, Fields.NONE)(fd, mode, implicitly[TupleConverter[Unit]])
    //TypedPipe.fromSingleField[T](pipe)(fd, mode).write(NullSource)
    //TypedPipe.from(pipe)
    })).unit
    //Execution.from(Stat(("write.parquet", "Coppersmith")).incBy(999))
  }

  /*
  def withCounter(name: String) = TypedPipeFactory({ (fd, mode) =>
    // The logic to drop down to cascading duplicates the (unfortunately private) method TypedPipe.onRawSingle
    val oldPipe = typedPipe.toPipe(new Fields(java.lang.Integer.valueOf(0)))(fd, mode, singleSetter)
    val id = CoppersmithStats.nextId.getAndIncrement()
    val newPipe = new Each(oldPipe, new Counter(CoppersmithStats.group, s"$id.$name"))
  })
   */

}

object HiveParquetSink {
  type DatabaseName = String
  type TableName    = String

  def apply[
    T <: ThriftStruct : Manifest : FeatureValueEnc,
    P : Manifest : PathComponents : TupleSetter
  ](
    dbName:    DatabaseName,
    tableName: TableName,
    tablePath: Path,
    partition: FixedSinkPartition[T, P]
  ): HiveParquetSink[T, P] = {
    val hiveTable = HiveTable[T, P](dbName, tableName, partition.underlying, tablePath.toString)

    val pathComponents = implicitly[PathComponents[P]].toComponents(partition.partitionValue)
    val partitionRelPath = new Path(partition.underlying.pattern.format(pathComponents: _*))

    HiveParquetSink[T, P](hiveTable, new Path(tablePath, partitionRelPath))
  }
}
