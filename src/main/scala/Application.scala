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
//    val samples = Array((Array("a"), Array(1, 1, 1, 1, 1)), // (1, 2, 3, 4, 5)
//                        (Array("b"), Array(1, 1, 1, 0, 1)), // (1, 2, 3, 5)
//                        (Array("c"), Array(1, 0, 1, 1, 1)), // (1, 3, 4, 5)
//                        (Array("d"), Array(1, 0, 1, 0, 1)), // (1, 3, 5)
//                        (Array("e"), Array(1, 0, 0, 1, 1))) // (1, 4, 5)
//
//    val compare = Array(samples(2)._1.zip(samples(4)._1), samples(2)._2.zip(samples(4)_2))
//    compare(0)
//    compare.foreach(println)
//    println(compare.filter(t => (t._1 == 0 && t._2 == 1)).isEmpty) // true면 _2 < _1
//    if ()

//    var hirarchy: Array[(Array[String], Array[Int])] = null
//
//    for (s <- samples) {
//
//    }

    // 1. 기본적인 Class 설정
    val morpho = new MorphoAnalysis(sc)
    val featureExtractor = new FeatureExtractor(sc)

    // Apriori 테스트
//    featureExtractor.apriori("input/apriori_data.txt", 10, 3, "output_apriori")

    val testDriver = new TestDriver(sc, morpho, featureExtractor)

    // Matrix 자동화 모듈
    // keyword가 존재하지 않을 경우(training_set)
    testDriver.run("input/data.txt", "output_entropy", null, "termFrequency")
    // keyword가 존재할 경우(test_set)
//    val keywords: RDD[String] = sc.textFile("input/trainingset_keyword_1000.txt").map(_.split(",")(0))
//    testDriver.run("input/data.txt", "output_entropy", keywords, "termFrequency")

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
