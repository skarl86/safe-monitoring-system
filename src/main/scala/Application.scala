import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by NCri on 15. 3. 3..
 */
object Application {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Safe Monitoring System").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val morpho = new MorphoAnalysis
//    val featureExtractor = new FeatureExtractor(sc)

//    morpho.makeKeywordInTweet("input/data.txt", "input/keyword.txt")

//    val corpus = sc.textFile("input/keyword.txt").map(_.split(",").toList).map(_.tail.toSeq)

//    val keywordTfidf = featureExtractor.tfidf(corpus, )

  }
}
