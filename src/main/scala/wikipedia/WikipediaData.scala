package wikipedia

case class WikipediaArticle(title: String, text: String, terms: Array[String])

object WikipediaData {
  def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text, text.split(' '))
  }
}
