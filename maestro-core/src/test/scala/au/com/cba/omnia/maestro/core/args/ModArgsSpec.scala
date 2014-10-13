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

package au.com.cba.omnia.maestro.core.args

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.GzipCodec

import com.twitter.scalding.{Args, Hdfs, Mode}

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

object ModArgsSpec extends Specification with ThrownExpectations { def is = s2"""

ModArgs laws
============
  modifying underlying hadoop config is safe $hadoopConfSafe
"""

  def hadoopConfSafe = {
    // not a scalacheck property because I didn't want to write an
    // arbitrary instance for hadoop Configuration,
    // manually specifying the universe of keys and their values
    val conf = new AuditableConf
    conf.record = (modified) => if (modified eq conf) failure("Original configuration was modified")
    val args = Mode.putMode(Hdfs(true, conf), Args("--hdfs"))
    ModArgs.compressOutput[GzipCodec].modify(args)
    ok
  }
}

class AuditableConf extends Configuration {
  var record: AnyRef => Unit = (_) => ()
  override def set(key: String, value: String): Unit = {
    record(this)
    super.set(key, value)
  }
  override def setClassLoader(classLoader: ClassLoader): Unit = {
    record(this)
    super.setClassLoader(classLoader)
  }

  override def setQuietMode(quietMode: Boolean): Unit = {
    record(this)
    super.setQuietMode(quietMode)
  }
}
