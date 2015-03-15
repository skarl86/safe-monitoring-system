import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Created by beggar3004 on 3/5/15.
 */
class TestDriver(sc: SparkContext,
                 morpho: MorphoAnalysis,
                 featureExtractor: FeatureExtractor) {

  def run(inputPath: String,
          outputPath: String,
          keywordPath: String = null,
          keywordMethod: String) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath,"./keyword/keyword_morpho.txt")
    val corpus: RDD[Seq[String]] = sc.textFile("keyword/keyword_morpho.txt").map(_.split(",").toSeq)
//    println("corpus counts : " + corpus.count())

    // 2. Term-Frequency로 키워드를 1000개 추출 => 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: RDD[String] = null

    keywordPath match {
      case null => {
        keywordMethod match {
          case "termFrequency" => {
            keywords =
              sc.parallelize(
                featureExtractor.termFrequency(corpus, false)
                  .take(1000)
                  .map(_._1)
              )
            // "tfkeywords_1000.txt" 파일에 저장.
            val arrayKeywords = keywords.collect()
            val writer = new PrintWriter(new File("input/tf_keywords_1000.txt"))
            for (keyword <- arrayKeywords) {
              writer.write(keyword + "\n")
            }
            writer.close()
          }
          case "tfidf" => {

          }

          case "entropy" => {

          }
        }
      }
      case _ => {
        keywords = sc.textFile("input/tf_keywords_1000.txt")
      }
    }

    // 3. Entropy를 수행
//    println("courpusClass : " + sc.textFile(inputPath).map(_.split(",")(0)).count())
    val tweetClass = sc.textFile(inputPath).map(_.split(",")(0)).collect()
    val tweet = corpus.map(_.mkString("")).collect()
    val corpusClass: RDD[(String, String)] = sc.parallelize(tweetClass.zip(tweet))
    val entropy = featureExtractor.entropy(corpusClass, keywords, false, true)
    entropy.coalesce(1).saveAsTextFile(outputPath)
  }

}
