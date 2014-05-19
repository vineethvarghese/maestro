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
