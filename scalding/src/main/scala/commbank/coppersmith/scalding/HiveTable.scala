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

// FIXME:
// This file is copied from maestro-scalding/src/main/scala/au/com/cba/omnia/maestro/hive/HiveTable.scala
// (specifically, the version in commit aa9146e), with `getAndResetCounters` changed to `getCounters`.
// Such a change is necessary to avoid clobbering coppersmith's own diagnostic counters.
// A more satisfactory solution would probably be to stop depending on HiveTable altogether,
// given that some of what it does is unnecessary -- especially given that coppersmith has its
// own solution to manage concurrently writing to the same partitions
// (see 4d3ee0d: "Multi set job support and improved sink commit semantics").

package commbank.coppersmith.scalding

import scalaz._, Scalaz._

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE

import com.twitter.scalding.{Hdfs => _, _}

import com.twitter.scrooge.ThriftStruct

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.{ParquetScroogeSource, PartitionParquetScroogeSource, PartitionParquetScroogeSink}
import au.com.cba.omnia.beeswax.Hive

import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.scalding.ConfHelper
import au.com.cba.omnia.maestro.scalding.ExecutionOps._

/** Base type for sourcing a hive table as [[TypedPipe]] */
sealed trait HiveTableSource[T <: ThriftStruct] {
  /** Fully qualified SQL reference to table.*/
  val name: String = s"$database.$table"

  /** Path of the table. */
  lazy val tablePath = new Path(externalPath.getOrElse(
    s"${(new HiveConf()).getVar(METASTOREWAREHOUSE)}/$database.db/$table"
  ))

  def database: String
  def table: String
  def externalPath: Option[String]

  /** Creates a scalding source to read from the hive table.*/
  def source: Mappable[T]
}

/**
  * Base type for hive table with write support.
  * ST - Type parameter of [[TypedSink]] and [[TypedPipe]] (Kept for backward compatibility)
  * returned in `sink` and `write` methods respectively.
  */
trait HiveTable[T <: ThriftStruct, ST] extends HiveTableSource[T] {
  /** Create [[Execution]] that writes the given [[TypedPipe]].*/
  def writeExecution(pipe: TypedPipe[T], append: Boolean = true): Execution[ExecutionCounters]
}

/**
  * Write support for a partitioned Hive Table.
  * P - Type of partition.
  */
class GenericPartitionedHiveTable[T <: ThriftStruct : Manifest, P : Manifest : TupleSetter](
  val database: String, val table: String, val fieldNames: List[String],
  val externalPath: Option[String],  val partitionBatchSize: Int = 100
) extends HiveTableSource[T] {
  /** List of partition column names and type (string by default). */
  val partitionMetadata    = fieldNames.map(n => (n, "string"))
  //Enforce the Hive partition pattern
  val hivePartitionPattern = fieldNames.map(_ + "=%s").mkString("/")
  val partitionGlob        = fieldNames.map(_ => "*").mkString("/")
  val hdfsPartitionGlob    = s"[^_.]$partitionGlob"

  override def source: PartitionParquetScroogeSource[P, T] =
    PartitionParquetScroogeSource[P, T](hivePartitionPattern, tablePath.toString)

  def writeToSink(pipe: TypedPipe[(P, T)], append: Boolean = true): Execution[ExecutionCounters] = {
    def modifyConfig(config: Config) = ConfHelper.createUniqueFilenames(config)

    def addPartitions(paths: List[Path]): Execution[Unit] =
      paths.grouped(partitionBatchSize)
        .map((ps: List[Path]) => Execution.hive(Hive.addPartitions(database, table, fieldNames, ps)))
        .toList.sequence_

    def withTable(ops: Path => Execution[ExecutionCounters]): Execution[ExecutionCounters] =
      // Work around: `withSeparateCache` avoids keeping references in the scalding EvalCache, to allow gc
      Execution.withSeparateCache(for {
       _             <- Execution.hive(Hive.createParquetTable[T](database, table, partitionMetadata, externalPath.map(new Path(_))))
       tablePath     <- Execution.hive(Hive.getPath(database, table))
       foldersBefore <- Execution.hdfs(Hdfs.glob(tablePath, hdfsPartitionGlob))
       counter       <- ops(tablePath)
       foldersAfter  <- Execution.hdfs(Hdfs.glob(tablePath, hdfsPartitionGlob))
       created        = foldersAfter.diff(foldersBefore)
       _             <- addPartitions(created)
      } yield counter)

    // Runs the scalding job and gets the counters
    def write(path: Path) = {
      println("you called what you wanted to call")
      for {
      b <- pipe
        .writeExecution(PartitionParquetScroogeSink[P, T](hivePartitionPattern, path.toString))
        .getCounters
        .map(_._2)
        _ = println(b.toMap)
      } yield b
    }

    if (append) {
      withTable(write(_).withSubConfig(modifyConfig)) 
    } else {
      /** Steps that happen below :-
        * 1) Create the folder if folder doesn't exist
        * 2) Get list of original parquet files
        * 3) Run the job
        * 4) Get the list of all updated parquet files, including obsolete
        * 5) Get files created by this job
        * 6) Get partitions updated by this job
        * 7) Get obsolete files: existed prior to this job, in partitions which have been updated
        * 8) Delete obsolete files
        */
      withTable { dst =>
        for {
          _            <- Execution.fromHdfs(Hdfs.mkdirs(dst))// Step 1
          oldFiles     <- Execution.fromHdfs(Hdfs.files(dst, "[^_]*", recursive=true))// Step 2
          counters     <- write(dst).withSubConfig(modifyConfig)// Step 3
          allFiles     <- Execution.fromHdfs(Hdfs.files(dst, "[^_]*", recursive=true))// Step 4
          newFiles      = allFiles.filterNot(oldFiles.toSet)// Step 5
          newPartitions = newFiles.map(_.getParent).distinct // Step 6
          toDelete      = oldFiles.filter(f => newPartitions.contains(f.getParent))// Step 7
          _            <- toDelete.map(p => Execution.fromHdfs(Hdfs.delete(p))).sequence//Step 8
        } yield counters
      }
    }
  }
}

/**
  * Information need to address/describe a specific partitioned hive table where partition
  * information can be derived from thrift data.
  */
case class PartitionedHiveTable[T <: ThriftStruct : Manifest, P : Manifest : TupleSetter](
  override val database: String,  override val table: String, partition: Partition[T, P],
  override val externalPath: Option[String] = None, override val partitionBatchSize: Int = 100
) extends GenericPartitionedHiveTable[T, P](database, table, partition.fieldNames, externalPath, partitionBatchSize)
  with HiveTable[T, (P, T)] {
  override def writeExecution(pipe: TypedPipe[T], append: Boolean = true): Execution[ExecutionCounters] = for {
    x <- writeToSink( pipe.map(r => (partition.extract(r) -> r)), append).getCounters
    _ = println("x._1: " + x._1.toMap.toString)
    _ = println("x._2: " + x._2.toMap.toString)
  } yield x._1
}

/** Information need to address/describe a specific unpartitioned hive table.*/
case class UnpartitionedHiveTable[T <: ThriftStruct : Manifest](
  database: String, table: String, externalPath: Option[String] = None
) extends HiveTable[T, T] {
  override def source =
    ParquetScroogeSource[T](tablePath.toString)

  /** Writes the contents of the pipe to this HiveTable. */
  override def writeExecution(pipe: TypedPipe[T], append: Boolean = true): Execution[ExecutionCounters] = {
    // Creates the hive table
    val setup = Execution.fromHive(
      Hive.createParquetTable[T](database, table, List.empty, externalPath.map(new Path(_))) >>
      Hive.getPath(database, table)
    )

    // Runs the scalding job and gets the counters
    def write(path: Path) = pipe
      .writeExecution(ParquetScroogeSource[T](path.toString))
      .getCounters
      .map(_._2)

    //If we are appending the files are written to a temporary location and then copied accross
    if (append) {
      def moveFiles(src: Path, dst: Path): Hdfs[Unit] = for {
        _     <- Hdfs.mkdirs(dst) // Create the folder if it doesn't already exist for some reason.
        files <- Hdfs.files(src, "*.parquet")
        uniq   =  UniqueID.getRandom.get
        _     <- files.zipWithIndex.traverse {
                   case (file, idx) => Hdfs.move(file, new Path(dst, f"part-$uniq-$idx%05d.parquet"))
                 }
      } yield ()

      setup.flatMap(dst => 
        Execution.fromHdfs(Hdfs.createTempDir()).bracket(
          tmpDir => Execution.fromHdfs(Hdfs.delete(tmpDir, true))
        )(tmpDir => 
          for {
            counters <- write(tmpDir)
            _        <- Execution.fromHdfs(moveFiles(tmpDir, dst))
          } yield counters
        ))
    } else {
      for {
        dst      <- setup
        counters <- write(dst)
      } yield counters
    }
  }
}

/** Constructors for HiveTable. */
object HiveTable {
  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B]
  ): HiveTable[A, (B, A)] =
    PartitionedHiveTable(database, table, partition, None)

  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], path: String
  ): HiveTable[A, (B,A)] =
    PartitionedHiveTable(database, table, partition, Some(path))

  /** Information need to address/describe a specific partitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    database: String, table: String, partition: Partition[A, B], 
    pathOpt: Option[String], partitionBatchSize: Int = 100
  ): HiveTable[A, (B, A)] =
    PartitionedHiveTable(database, table, partition, pathOpt, partitionBatchSize)

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String
  ): HiveTable[A, A] =
    UnpartitionedHiveTable(database, table, None)

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, path: String
  ): HiveTable[A, A] =
    new UnpartitionedHiveTable(database, table, Some(path))

  /** Information need to address/describe a specific unpartitioned hive table.*/
  def apply[A <: ThriftStruct : Manifest](
    database: String, table: String, pathOpt: Option[String]
  ): HiveTable[A, A] =
    UnpartitionedHiveTable(database, table, pathOpt)
}
