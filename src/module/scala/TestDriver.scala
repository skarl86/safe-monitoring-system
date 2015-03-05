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
          keywordMethod: String) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath,"./keyword/keyword_morpho.txt")
    val corpus: RDD[Seq[String]] = sc.textFile("keyword/keyword_morpho.txt").map(_.split(",").toSeq)

    // 2. Term-Frequency로 키워드를 1000개 추출 => 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: RDD[String] = null
    keywordMethod match {
      case "termFrequency" => {
        keywords =
          sc.parallelize(
            featureExtractor.termFrequency(corpus, false)
              .take(1000)
              .map(_._1)
          )
      }
      case "tfidf" => {

      }

      case "entropy" => {

      }
    }

    // 3. Entropy를 수행
    val corpusClass: RDD[(String, String)] = sc.textFile(inputPath).map(_.split("\t")).map(list => (list(0), list(1)))
    val entropy = featureExtractor.entropy(corpusClass, keywords, false, true)
    entropy.coalesce(1).saveAsTextFile(outputPath)
  }

}
