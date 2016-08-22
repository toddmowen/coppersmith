//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.scalding

import scala.util.{Try, Success, Failure}

import com.twitter.scalding.typed.{TypedPipe, TypedPipeFactory}
import com.twitter.scalding.TupleSetter.singleSetter
import com.twitter.scalding.{Execution, ExecutionCounters}

import cascading.tuple.Fields
import cascading.pipe.Each
import cascading.operation.state.Counter

object CoppersmithStats {
  val group = "Coppersmith"

  // Don't use getLogger(getClass()) pattern, as the class name is ugly ("$" suffix for companion object)
  val log = org.slf4j.LoggerFactory.getLogger("commbank.coppersmith.scalding.CoppersmithStats")

  // Prepending a unique ID to each counter name prevents name clashes, and provides a somewhat sensible ordering.
  // Even if we create a million counters per second, it will take 290,000+ years to overflow a Long.
  val nextId = new java.util.concurrent.atomic.AtomicLong(1)

  implicit def fromTypedPipe[T](typedPipe: TypedPipe[T]) = new CoppersmithStats(typedPipe)

  /** Run the [[com.twitter.scalding.Execution]], logging coppersmith counters after completion. */
  def logCountersAfter[T](exec: Execution[T]): Execution[T] = {
    val tryExecution: Execution[Try[T]] =
      exec.map{ Success(_) }.recoverWith{ case throwable: Throwable => Execution.from[Try[T]](Failure(throwable)) }

    for {
      (result, counters) <- tryExecution.getCounters
      _                  <- Execution.from(logCounters(counters))
    } yield result.get  // any exception caught by the above recoverWith is rethrown here
  }

  /** Log (at INFO level) all coppersmith counters found in the passed [[com.twitter.scalding.ExecutionCounters]]. */
  def logCounters(counters: ExecutionCounters): Unit = {
    if (counters.keys.isEmpty) {
      log.warn("Hadoop counters not available (usually caused by job failure)")
    }
    else {
      log.info("Coppersmith counters:")
      fromCounters(counters).foreach { case (name, value) =>
        log.info(f"    ${name}%-30s ${value}%10d")
      }
    }
  }

  def fromCounters(counters: ExecutionCounters): List[(String, Long)] = {
    println(counters.toMap)
    counters.keys.filter(_.group == group).map { key =>
      val Array(id, name) = key.counter.split(raw"\.", 2)
      (id.toLong, name, key)
    }.toList.sortBy(_._1).map { case (id, name, key) => (name, counters(key)) }
  }
}

class CoppersmithStats[T](typedPipe: TypedPipe[T]) extends {
  /** Calling this on any [[com.twitter.scalding.typed.TypedPipe]] will cause a counter with the given name
    * to be incremented for every tuple that is read from the pipe. */
  def withCounter(name: String) = TypedPipeFactory({ (fd, mode) =>
    // The logic to drop down to cascading duplicates the (unfortunately private) method TypedPipe.onRawSingle
    val oldPipe = typedPipe.toPipe(new Fields(java.lang.Integer.valueOf(0)))(fd, mode, singleSetter)
    val id = CoppersmithStats.nextId.getAndIncrement()
    val newPipe = new Each(oldPipe, new Counter(CoppersmithStats.group, s"$id.$name"))
    TypedPipe.fromSingleField[T](newPipe)(fd, mode)
  })
}
