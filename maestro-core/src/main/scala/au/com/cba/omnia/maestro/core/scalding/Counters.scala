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

package au.com.cba.omnia.maestro.core.scalding

import scala.collection.JavaConverters._

import com.twitter.scalding.Job

import cascading.cascade.Cascade
import cascading.flow.StepCounters
import cascading.stats.CascadingStats
import cascading.stats.CascadingStats.Status

/**
  * Convienient access to stats for a scalding `CascadeJob`
  *
  * Counters should be created and used in the `CascadeJob.postProcessCascade`
  * method. Example usage:
  *
  * {{{
  * import com.twitter.scalding.CascadeJob
  * import cascading.cascade.Cascade
  *
  * class MyCascadeJob(args: Args) extends CascadeJob(args) {
  *   val jobs = Seq(myJob1, myJob2) // my jobs defined elsewhere
  *   override def postProcessCascade(cascade: Cascade) {
  *     val job1Counters  = Counters.job(cascade, myJob1)
  *     val job2Counters  = Counters.job(cascade, myJob2)
  *     val totalCounters = Counters.cascade(cascade)
  *
  *     println(s"Job 1 read ${job1Counters.tuplesRead} rows")
  *     println(s"Job 2 wrote ${job2Counters.tuplesWritten} rows")
  *     println(s"Job 2 status: ${job2Counters.staus}")
  *     println
  *
  *     println("Overall cascade stats:")
  *     totalCounters.counters.foreach { case Counter(name, group, counter, value) => {
  *       println(s"$group $counter = $value")
  *     }}
  *     println
  *   }
  * }
  * }}}
  *
  * The statistics underlying the counters class are mutable, so you should
  * not look at any results until after the job has finished processing.
  */
case class Counters(stats: CascadingStats) {
  // we lookup stats afresh every time the methods are called,
  // as the underlying values are mutable.

  /** The number of tuples read, */
  def tuplesRead: Long    = stats.getCounterValue(StepCounters.Tuples_Read)

  /** The number of tuples written. */
  def tuplesWritten: Long = stats.getCounterValue(StepCounters.Tuples_Written)

  /** The status of the job */
  def status: Status      = stats.getStatus

  /** The value of a single counter */
  def counterValue(group: String, counter: String): Long =
    stats.getCounterValue(group, counter)

  /** A list of all counters */
  def counters: Iterable[Counter] = for {
    group   <- stats.getCounterGroups.asScala
    counter <- stats.getCountersFor(group).asScala
  } yield Counter(stats.getName, group, counter, counterValue(group, counter))
}

/** Factory methods for Counters. */
object Counters {

  /** Get the counters for the entire cascade */
  def cascade(cascade: Cascade): Counters =
    Counters(cascade.getCascadeStats)

  /** Get the counters for a particular job */
  def job(cascade: Cascade, job: Job): Counters = {
    val cascadeStats = cascade.getCascadeStats
    val allJobStats  = cascadeStats.getChildren.asScala.asInstanceOf[Iterable[CascadingStats]]

    // throwing an exception instead of returning optional type
    // as we don't expect users to pass in jobs that aren't in the cascade job list
    val jobStats = allJobStats
      .find(stats => stats.getName == job.name)
      .getOrElse(throw new Exception(s"stats not found for job ${job.name}"))

    Counters(jobStats)
  }

  /** Get the counters for all individual jobs */
  def jobs(cascade: Cascade): List[Counters] = {
    val cascadeStats = cascade.getCascadeStats
    val allJobStats = cascadeStats.getChildren.asScala.asInstanceOf[Iterable[CascadingStats]]
    allJobStats.map(Counters(_)).toList
  }
}

/** An individual counter */
case class Counter(name: String, group: String, counter: String, value: Long)
