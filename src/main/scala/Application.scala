/**
 * Created by NCri on 15. 3. 3..
 */
object Application {
  def main(args: Array[String]) {
    val morpho = new MorphoAnalysis
    val tfidf = new TFIDF

    morpho.makeKeywordInTweet("Input path", "Output path")
    tfidf.run
  }
}
