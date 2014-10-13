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

object ConfigSpec extends Specification { def is = s2"""
Config properties
============
  configuration retrieves the existing config from the Hdfs mode  $getHdfsConfig
  configuration creates a config if it can't find an existing one $fallbackToFreshConfig

"""

  def getHdfsConfig = {
    val conf = new Configuration
    conf.set("basedir", "mybasedir")
    val args = Mode.putMode(Hdfs(true, conf), Args("--hdfs"))
    ConfigLogic.configuration(args) must be(conf)
    ConfigLogic.configuration(args).get("basedir") must_== "mybasedir"
  }

  def fallbackToFreshConfig = {
    val args = Args("--hdfs")
    ConfigLogic.configuration(args) must not(beNull or throwA[Exception])
  }
}

object ConfigLogic extends Config
