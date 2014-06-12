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

import sbt._

object GenUnravelPipes {
  val max = 22

  def gen(dir: File) = {
    val dst = dir / "au" / "com" / "cba" / "maestro" / "core" / "scalding" / "UnravelPipes.scala"
    IO.write(dst,
      s"""package au.com.cba.omnia.maestro.core
         |package scalding

         |import com.twitter.scalding.TypedPipe

         |object UnravelPipeImplicits extends UnravelPipeImplicits

         |trait UnravelPipeImplicits {
         |  ${(2 to max).map(genImplicits).mkString("\n  ")}
         |}

         |${(2 to max).map(genSyntax).mkString("\n\n")}
      """.stripMargin)

    Seq(dst)
  }


  def genImplicits(i: Int): String = {
    val types = (1 to i).map(j => s"I$j").mkString(", ")
    s"""implicit def TypedPipeToUnravelPipe$i[$types](pipe: TypedPipe[($types)]): UnravelPipe$i[$types] = UnravelPipe$i[$types](pipe)"""
  }

  def genSyntax(i: Int): String = {
    val types = (1 to i).map(j => s"I$j").mkString(", ")
    val funcs = (1 to i).map(j => s"f$j: TypedPipe[I$j] => O$j").mkString(", ")
    val outTypes = (1 to i).map(j => s"O$j").mkString(", ")
    val applications = (1 to i).map(j => s"f$j(forked.map(_._${j}))").mkString(",\n    ")

    s"""case class UnravelPipe$i[$types](pipe: TypedPipe[($types)]) {
       |  val forked = pipe.fork
       |  def >>*[$outTypes]($funcs): ($outTypes) = (
       |    $applications
       |  )
       |}""".stripMargin

  }
}
