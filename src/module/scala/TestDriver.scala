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
          numOfKeyword: Int = 0,
          keywordMethod: String) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath,"./keyword/keyword_morpho.txt")
    val corpus: RDD[Seq[String]] = sc.textFile("keyword/keyword_morpho.txt").map(_.split(",").toSeq)

    // 2. Term-Frequency로 키워드를 1000개 추출 => 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: RDD[String] = null

    keywordPath match {
      case null => {
        keywordMethod match {
          case "termFrequency" => {
            var keywords: Array[(String, Double)] = null
            if (numOfKeyword == 0)
              keywords = featureExtractor.termFrequency(corpus, false).collect()
            else
              keywords = featureExtractor.termFrequency(corpus, false).take(numOfKeyword)
            // "tfkeywords_1000.txt" 파일에 저장.
            val writer = new PrintWriter(new File("input/tf_keywords.txt"))
            for (keyword <- keywords)
              writer.write(keyword._1 + "," + keyword._2 + "\n")
            writer.close()
          }

          case "tfidf" => {

          }

          case "entropy" => {

          }
        }
      }

      case _ => {
        keywords = sc.textFile("input/tf_keywords.txt")
      }
    }

    // 3. Entropy를 수행
//    val tweetClass = sc.textFile(inputPath).map(_.split(",")(0)).collect()
//    val tweet = corpus.map(_.mkString(" ")).collect()
//    val corpusClass: RDD[(String, String)] = sc.parallelize(tweetClass.zip(tweet))
//    val entropy = featureExtractor.entropy(corpusClass, keywords, false, true)
//    entropy.coalesce(1).saveAsTextFile(outputPath)

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

  def runTfidf(inputPath: String,
               keywordPath: String = null,
               numOfKeyword: Int = 0) = {

    // 1. 형태소 분석기를 통하여 기존의 문서를 다시 생성.
    morpho.makeRDDKeywordInTweet(inputPath,"./keyword/keyword_morpho.txt")
    val classes = sc.textFile(inputPath).map(_.split(",")(0))
    val corpus: RDD[Seq[String]] = sc.textFile("keyword/keyword_morpho.txt").map(_.split(",").toSeq)

    // 2. Term-Frequency로 키워드를 1000개 추출 => 사용자가 원하는 특징점에 따라 수행하도록 추후 바꾸자.
    var keywords: Array[String] = null

    keywordPath match {
      case null => {
        var keywordsTF: Array[(String, Double)] = null
        if (numOfKeyword == 0)
          keywordsTF = featureExtractor.termFrequency(corpus, false).collect()
        else
          keywordsTF = featureExtractor.termFrequency(corpus, false).take(numOfKeyword)
        // "tf_keywords.txt" 파일에 저장.
        val writer = new PrintWriter(new File("input/tf_keywords.txt"))
        for (keyword <- keywordsTF)
          writer.write(keyword._1 + "," + keyword._2 + "\n")
        writer.close()

        keywords = keywordsTF.map(_._1)
      }

      case _ => {
        keywords = sc.textFile("input/tf_keywords.txt").collect()
      }
    }

    val corpusTfidf = featureExtractor.tfidfByDocument(corpus)
    val corpusTfidfClasses = corpusTfidf.collect().zip(classes.collect())

    val writer = new PrintWriter(new File("matrix_tfidf.csv"))
    writer.write(keywords.mkString(",") + ",class\n")

    for (document <- corpusTfidfClasses) {
      for (tuple <- document._1) {
        for (keyword <- keywords) {
          if (tuple._1.equals(keyword))
            writer.write(tuple._2 + 1 + ",")
          else writer.write("0,")
        }
        writer.write(document._2 + "\n")
      }
    }

    writer.close()

  }

}
