import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Created by beggar3004 on 3/5/15.
 */
class TestDriver(sc: SparkContext,
                 morpho: MorphoAnalysis,
                 featureExtractor: FeatureExtractor) {

  def createTrainingMatrix(inputPath: String,
          outputPath: String,
          keywordOutputPath1: String = null,
          keywordOutputPath2: String = null,
          firstNumOfKeyword: Int = 0,
          secondNumOfKeyword: Int = 0,
          keywordMethod: String) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath, "input/data_morpho.txt")
    val corpus: RDD[Seq[String]] = sc.textFile("input/data_morpho.txt").map(_.split(",").toSeq)
    val classOfTweet: RDD[String] = sc.textFile(inputPath).map(_.split(",")(0))
    val classCorpus: Array[(String, Seq[String])] =
      classOfTweet.collect().zip(corpus.collect())

    println("-------------- Phase 1. Document 생성 완료 --------------")

    // 2. 첫번째 키워드 목록 추출 : 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: Array[String] = null

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

    // 2-1. 첫번째 Keyword 목록의 파일 작성 여부 판단.
    keywordOutputPath1 match {
      case path => {
        val writer = new PrintWriter(new File(path))
        for (keyword <- keywordsAndValue)
          writer.write(keyword._1 + "," + keyword._2 + "\n")
        writer.close()
      }

      case null => {

      }
    }

    keywords = keywordsAndValue.map(_._1)

    println("-------------- Phase 2. 첫번째 키워드 목록 추출 완료 --------------")

    // 3. Entropy keywords 추출.
    val entropyKeywords: Array[(String, Double)] =
      if (secondNumOfKeyword == 0)
        featureExtractor.entropyOfKeywords(classCorpus, keywords, false).collect()
      else
        featureExtractor.entropyOfKeywords(classCorpus, keywords, false).take(secondNumOfKeyword)

    keywordOutputPath2 match {
      case path => {
        val writer = new PrintWriter(new File(path))
        for (keyword <- entropyKeywords)
          writer.write(keyword._1 + "," + keyword._2 + "\n")
        writer.close()
      }

      case null => {

      }
    }

    println("-------------- Phase 3. 두번째 키워드 목록 추출 완료 --------------")

    // 4. Matrix 생성.
    featureExtractor.createMatrix(classCorpus, entropyKeywords.map(_._1), outputPath)

    println("-------------- Phase 4. Matrix 생성 완료 --------------")

  }

}