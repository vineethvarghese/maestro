package au.com.cba.omnia.maestro.core
package clean

import au.com.cba.omnia.maestro.core.data._

case class Clean(run: (Field[_, _], String) => String)

object Clean {
  def all(cleans: Clean*): Clean =
    Clean((field, data) => cleans.foldLeft(data)((acc, clean) => clean.run(field, acc)))

  def trim: Clean =
    Clean((_, data) => data.trim)

  def removeNonPrintables: Clean =
    Clean((_, data) => data.replaceAll("[^\\p{Print}]", ""))
}
