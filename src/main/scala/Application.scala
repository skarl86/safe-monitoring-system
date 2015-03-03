import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD


/**
 * Created by NCri on 15. 3. 3..
 */
object Application {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Safe Monitoring System").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val morpho = new MorphoAnalysis(conf, sc)
    val featureExtractor = new FeatureExtractor(sc)

    morpho.makeRDDKeywordInTweet("./input/Baseball.txt","./input/keyword.txt")

//    morpho.makeKeywordInTweet("input/data.txt", "input/keyword.txt")

    val corpus: RDD[Seq[String]] = sc.textFile("input/keyword.txt").map(_.split(",").toSeq)
//    val corpus: RDD[(String, String)] = sc.textFile("input/data.txt").map(_.split("\t")).map(list => (list(0), list(1)))
//    val keywords: RDD[String] = sc.textFile("input/keyword.txt").map(_.split(",").toSeq).flatMap(_.tail).distinct

//    corpus.foreach(println)

    val keywordTfidf = featureExtractor.tfidf(corpus,"average","tfidf",false)
    keywordTfidf.coalesce(1).saveAsTextFile("output_spark")

//    corpus.filter(_._2.contains("설정")).foreach(println)

//    val entropy = featureExtractor.entropy(corpus, keywords)
//    entropy.saveAsTextFile("output")
  }
}
