package au.com.cba.omnia.maestro.core
package filter

case class RowFilter(run: List[String] => Option[List[String]])

object RowFilter {
  def keep: RowFilter =
    RowFilter(Some.apply)

  def byRowLeader(include: String): RowFilter =
    RowFilter(row => if (row.headOption.exists(_ == include)) Some(row.tail) else None)
}
