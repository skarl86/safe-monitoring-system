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
          keywordInputPath: String = null,
          keywordOutputPath: String = null,
          firstNumOfKeyword: Int = 0,
          secondNumOfKeyword: Int = 0,
          keywordMethod: String) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath, "input/data_morpho.txt")
    val corpus: RDD[Seq[String]] = sc.textFile("input/data_morpho.txt").map(_.split(",").toSeq)
    val classOfTweet: RDD[String] = sc.textFile(inputPath).map(_.split(",")(0))
    val classCorpus: Array[(String, Seq[String])] =
      classOfTweet.collect().zip(corpus.collect())

    // 2. 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: RDD[String] = null

    keywordInputPath match {
      case null => {
        var keywordsAndValue: Array[(String, Double)] = null

        keywordMethod match {
          case "tf" => {
            // 2-1. TF가 높은 순서대로 정렬하고 특정 개수만큼 추출.
            if (firstNumOfKeyword == 0)
              keywordsAndValue = featureExtractor.termFrequencyOfKeywords(corpus, false).collect()
            else
              keywordsAndValue = featureExtractor.termFrequencyOfKeywords(corpus, false).take(firstNumOfKeyword)
          }

          case "tfidf" => {
            // 2-2. TFIDF가 높은 순서대로 정렬하고 특정 개수만큼 추출.
            if (firstNumOfKeyword == 0)
              keywordsAndValue = featureExtractor.tfidfOfKeywords(corpus, "average", false).collect()
            else
              keywordsAndValue = featureExtractor.tfidfOfKeywords(corpus, "average", false).take(firstNumOfKeyword)
          }

        }

        // Keyword들을 파일에 작성.
        val writer = new PrintWriter(new File(keywordOutputPath))
        for (keyword <- keywordsAndValue)
          writer.write(keyword._1 + "," + keyword._2 + "\n")
        writer.close()

        keywords = sc.parallelize(keywordsAndValue.map(_._1))

      }

      case path => {
        keywords = sc.textFile(path)
      }
    }

    // 3. Entropy keywords 추출.
    val entropyKeywords: Array[String] =
      if (secondNumOfKeyword == 0)
        featureExtractor.entropyOfKeywords(sc.parallelize(classCorpus), keywords, false).map(_._1).collect()
      else
        featureExtractor.entropyOfKeywords(sc.parallelize(classCorpus), keywords, false).map(_._1).take(secondNumOfKeyword)

    // 4. Matrix 생성.
    featureExtractor.createMatrix(classCorpus, entropyKeywords, outputPath)

  }

}