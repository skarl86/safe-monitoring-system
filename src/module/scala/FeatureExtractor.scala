import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


/**
 * Start Date : 2015/02/27
 * Author : Jinyung Hong
 * To-do Lists
 * 1.
 */

class FeatureExtractor(sc: SparkContext) {

  def tfidf(corpus: RDD[Seq[String]],
            method: String = "average",
            sort: String = "count",
            desending: Boolean = false) = {

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(corpus)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // 1. 각 키워드 Count
    val keywordCount: RDD[(String, Int)] = corpus.flatMap(_.map((_, 1))).reduceByKey(_ + _)

    // IDF
    val keywordIdf = corpus.flatMap(seq => seq.map(str => (str, idf.idf(hashingTF.indexOf(str)))))

    val keywordIdf2 =
      keywordIdf.mapValues((_, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)

    // 2. 각 키워드별 TF-IDF 매칭 작업.
    val keywordIndex = corpus.map(seq => seq.map(str => (str, hashingTF.indexOf(str))))

    val keywordTfidf: RDD[(String, Double)] =
      keywordIndex.zip(tfidf).flatMap(t1 => t1._1.map(t2 => (t2._1, t1._2.toArray(t2._2))))

    var reduceKeyword: RDD[(String, Double)] = null
    // 3. 추후 Average말고 다른 Method를 추가 가능.
    method match {
      case "average" =>
        reduceKeyword =
          keywordTfidf.mapValues((_, 1))
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
          .mapValues(x => x._1 / x._2)
    }

    // 4. Sort 방법, asending / descending
//    var keywordCountTfidf: RDD[(String, (Int, Double))] = null

    val keywordCountTfidf = keywordCount.join(reduceKeyword).join(keywordIdf2).sortBy(_._2._2, desending)
//    sort match  {
//      case "count" =>
//        keywordCountTfidf =
//          keywordCount.join(reduceKeyword).sortBy(_._2._1, desending)
//
//      case "tfidf" =>
//        keywordCountTfidf =
//          keywordCount.join(reduceKeyword).sortBy(_._2._2, desending)
//    }

    keywordCountTfidf

  }

  def entropy(classCorpus: RDD[(String, String)],
              keywords: RDD[String]) = {

    val COLLECTION_VALUE = 0.0000000000000001
    val TWEET_TOTAL_COUNT = classCorpus.count()

    var keywordInfo = List(("start", (.0, .0, .0), (.0, .0, .0)))

    val yesTweetCount = classCorpus.filter(x => x._1.equals("p")).count() + COLLECTION_VALUE
    val noTweetCount = classCorpus.filter(x => x._1.equals("n")).count() + COLLECTION_VALUE
    val totalTweetCount = yesTweetCount + noTweetCount

    for (keyword <- keywords.collect()) {

      val yesFilteredTweet = classCorpus.filter(_._2.contains(keyword))
      val noFilteredTweet =classCorpus.filter(t => !(t._2.contains(keyword)))

      val yesTotalCount = yesFilteredTweet.count()
      val noTotalCount = TWEET_TOTAL_COUNT - yesTotalCount

      val yesSide = yesFilteredTweet.map(list => (yesTotalCount, keyword, list._1, list._2))
      val noSide = noFilteredTweet.map(list => (noTotalCount, keyword, list._1, list._2))

      val yesYesCount = yesSide.filter(list => list._3.equals("p")).count()
      val yesNoCount = yesTotalCount - yesYesCount

      val noYesCount = noSide.filter(list => list._3.equals("p")).count()
      val noNoCount = noTotalCount - noYesCount

      keywordInfo = keywordInfo :+ (keyword, (yesTotalCount.toDouble+COLLECTION_VALUE,
                                              yesYesCount.toDouble+COLLECTION_VALUE,
                                              yesNoCount.toDouble+COLLECTION_VALUE),
                                              (noTotalCount.toDouble+COLLECTION_VALUE,
                                                noNoCount.toDouble+COLLECTION_VALUE,
                                                noYesCount.toDouble+COLLECTION_VALUE))
    }

    keywordInfo = keywordInfo match {
      case x :: xs => xs
    }

    def log2(x: Double) = math.log(x) / math.log(2)

    val final_data = keywordInfo.map(line => line match {
      case (key, (yt, yy, yn), (nt, nn, ny)) => {
        val entropy = ( (-yesTweetCount / totalTweetCount) * log2(yesTweetCount / totalTweetCount) +
          (-noTweetCount / totalTweetCount) * log2(noTweetCount / totalTweetCount) ) -
          ( (yt / TWEET_TOTAL_COUNT) * ( ( (-yy / yt) * log2(yy / yt)) + ( (-yn / yt) * log2(yn / yt) ) ) +
            (nt / TWEET_TOTAL_COUNT) * ( ( (-nn / nt) * log2(nn / nt)) + ( (-ny / nt) * log2(ny / nt) ) ) )
        (key, entropy)
      }
    }).sortBy(x => x._2)

    val keywordEntropy = sc.parallelize(final_data)

    keywordEntropy

  }

}
