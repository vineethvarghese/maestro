
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
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.example.thrift.Customer
import au.com.cba.omnia.parlour.{ ExportSqoopConsoleJob, ExportSqoopJob, ImportSqoopConsoleJob }
import au.com.cba.omnia.parlour.SqoopSyntax.TeradataParlourExportDsl
import com.twitter.scalding.NullTap
import au.com.cba.omnia.parlour.ImportSqoopJob
import au.com.cba.omnia.parlour._

class DummyTap(id: String) extends NullTap {
  override def getIdentifier = id
}

class SqoopExportCascade(args: Args) extends MaestroCascade[Customer](args) {
  val jobs = Seq(new ExportSqoopConsoleJob(args))
}

class SqoopImportCascade(args: Args) extends MaestroCascade[Customer](args) {
  val jobs = Seq(new ImportSqoopConsoleJob(args))
}
