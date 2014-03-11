package au.com.cba.omnia.maestro.core
package filter

case class RowFilter(run: List[String] => Boolean)

object RowFilter {
  def keep: RowFilter =
    RowFilter(_ => true)

  def byRowLeader(include: String): RowFilter =
    RowFilter(_.headOption.exists(_ == include))
}
