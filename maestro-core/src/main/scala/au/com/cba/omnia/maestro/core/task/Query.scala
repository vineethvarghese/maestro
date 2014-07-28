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

package au.com.cba.omnia.maestro.core.task

import scala.io.Source

import org.apache.hadoop.hive.conf.HiveConf

import com.twitter.scalding.{Args, Job}

import au.com.cba.omnia.ebenezer.scrooge.hive.HiveJob

import au.com.cba.omnia.maestro.core.hive.HiveTable

/** A trait for API to run hive queries.*/
trait Query {
  /** 
    * Runs the specified hive query.
    * 
    * The input and output tables are used to determine dependencies for scheduling.
    */
  def hiveQuery(
    args: Args, name: String, query: String,
    inputs: List[HiveTable[_, _]], output: Option[HiveTable[_, _]], conf: HiveConf
  ) : Job =
    HiveJob(args, name, query, inputs.map(_.source(conf)), output.map(_.sink(conf)))

  /**
    * Runs the specified hive query.
    * 
    * The input and output tables are used to determine dependencies for scheduling.
    */
  def hiveQuery(
    args: Args, name: String, query: String,
    input: HiveTable[_, _], output: Option[HiveTable[_, _]], conf: HiveConf
  ) : Job =
    HiveJob(args, name, query, input.source(conf), output.map(_.sink(conf)))
}
