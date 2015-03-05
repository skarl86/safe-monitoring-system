import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by NCri on 15. 3. 3..
 */
object Application {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Safe-Monitoring-System").setMaster("local[*]")
    val sc = new SparkContext(conf)
//
    val morpho = new MorphoAnalysis(sc)
    val featureExtractor = new FeatureExtractor(sc)
    val testDriver = new TestDriver(sc, morpho, featureExtractor)

    testDriver.run("input/data.txt", "output_entropy", "termFrequency")

//    morpho.makeRDDKeywordInTweet("./input/data.txt","./output/output.txt")

    // TFIDF corpus
//    val corpus: RDD[Seq[String]] = sc.textFile("output/output.txt").map(_.split(",").toSeq)
    // Entropy corpus
//    val corpus: RDD[(String, String)] = sc.textFile("input/data.txt").map(_.split("\t")).map(list => (list(0), list(1)))
//    val keywords: RDD[String] = sc.textFile("input/trainingset_keyword_1000.txt").map(_.split(",")(0))
    // Entropy Matrix
//    val corpus: RDD[(String, String)] = sc.textFile("input/test_data.txt").map(_.split("\t")).map(list => (list(0), list(1)))
//    val keywords: RDD[String] = sc.textFile("input/entropy_keyword.txt").map(_.split(",")(0))

////    corpus.foreach(println)
//
//    val keywordTfidf = featureExtractor.tfidf(corpus,"average","tfidf",false)
//    keywordTfidf.coalesce(1).saveAsTextFile("output_spark")

//    val keywordTF = featureExtractor.termFrequency(corpus, false)
//    keywordTF.coalesce(1).saveAsTextFile("output_tf")

//    corpus.filter(_._2.contains("설정")).foreach(println)

//    val entropy = featureExtractor.entropy(corpus, keywords, false, true)
//    entropy.coalesce(1).saveAsTextFile("output_entropy")

//    featureExtractor.entropyMatrix(corpus, keywords)
  }
}
