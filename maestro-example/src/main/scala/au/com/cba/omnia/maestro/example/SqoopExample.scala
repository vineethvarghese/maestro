
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

package au.com.cba.omnia.maestro.example

import com.twitter.scalding.Args

import au.com.cba.omnia.maestro.api.MaestroCascade
import au.com.cba.omnia.maestro.example.thrift.Customer
import au.com.cba.omnia.parlour.{ExportSqoopJob, ImportSqoopConsoleJob}
import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

class SqoopExportCascade(args: Args) extends MaestroCascade[Customer](args) {
  val sqoopOptions = ParlourExportDsl()
  .connectionString("adsfas")
  .username("asdfadsf")
  .exportDir("asdfasdf")
  .tableName("")
  .toSqoopOptions
  
  val jobs = Seq(new ExportSqoopJob(sqoopOptions)(args))
}

class SqoopImportCascade(args: Args) extends MaestroCascade[Customer](args) {
  val jobs = Seq(new ImportSqoopConsoleJob(args))
}