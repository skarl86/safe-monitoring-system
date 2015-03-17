import java.io.{File, PrintWriter}
import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Start Date : 2015/02/27
 * Author : Jinyung Hong
 * To-do Lists
 * 1.
 */

class FeatureExtractor(sc: SparkContext) {

  private val _db = "jdbc:mysql://218.54.47.24:3306/tweet?user=root&password=tkfkdgo1_"

  def getKeywordFromDB(): RDD[String] ={
    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement()
    var keywords:ListBuffer[String] = new ListBuffer()

    try {
      val resultSet = statement.executeQuery(
        "SELECT keyword FROM keyword")
      while(resultSet.next()){
        keywords += resultSet.getString("keyword")
      }
    } catch{
      case e => e.printStackTrace
    }

    sc.parallelize(keywords.toList)
  }

  def termFrequencyOfKeywords(corpus: RDD[Seq[String]],
                    sort: Boolean = false): RDD[(String, Double)] = {

    val keywordCount: RDD[(String, Int)] =
      corpus.flatMap(_.map((_, 1)))
        .reduceByKey(_ + _)

    val tf: RDD[Int] = keywordCount.map(_._2)

    val tfMean = tf.mean()
    val tfRange = tf.max() - tf.min()
    // Mean Normalization 적용
    // -1 <= tf <= 1
    val normalKeywordCount =
      keywordCount
        .map(t => (t._1, (t._2 - tfMean) / tfRange))
        .sortBy(_._2, sort)

    normalKeywordCount

  }

  def tfidfOfDocuments(corpus: RDD[Seq[String]]): RDD[Seq[(String, Double)]] = {

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(corpus)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // 1. 각 키워드 Count
    val keywordCount: RDD[(String, Int)] = corpus.flatMap(_.map((_, 1))).reduceByKey(_ + _)

    // 2. 각 키워드별 TF-IDF 매칭 작업.
    val keywordIndex = corpus.map(seq => seq.map(str => (str, hashingTF.indexOf(str))))

    val tfidfOfDocuments =
      keywordIndex.zip(tfidf)
        .map(t1 => t1._1.map(t2 => (t2._1, t1._2.toArray(t2._2))))

    tfidfOfDocuments

  }

  def tfidfOfKeywords(corpus: RDD[Seq[String]],
            method: String = "average",
            sort: Boolean = false): RDD[(String, Double)] = {

    val keywordTfidf: RDD[(String, Double)] = tfidfOfDocuments(corpus).flatMap(_.toList)

    var reduceKeywordTfidf: RDD[(String, Double)] = null
    // 추후 Average말고 다른 Method를 추가 가능.
    method match {
      case "average" => {
        reduceKeywordTfidf =
          keywordTfidf.mapValues((_, 1))
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
            .mapValues(x => x._1 / x._2).sortBy(_._2, sort)
      }

      case "std" => {

      }
    }

    reduceKeywordTfidf

  }

  def entropyOfKeywords(classCorpus: Array[(String, Seq[String])],
              keywords: Array[String],
              sort: Boolean = false): RDD[(String, Double)] = {

    val classCorpusString = classCorpus.map(t => (t._1, t._2.mkString(" ")))
    val COLLECTION_VALUE = 0.0000000000000001
    val TWEET_TOTAL_COUNT = classCorpusString.length

    var keywordInfo = List(("start", (.0, .0, .0), (.0, .0, .0)))

    val yesTweetCount = classCorpusString.filter(x => x._1.equals("p")).length + COLLECTION_VALUE
    val noTweetCount = classCorpusString.filter(x => x._1.equals("n")).length + COLLECTION_VALUE
    val totalTweetCount = yesTweetCount + noTweetCount

    for (keyword <- keywords) {

      val yesFilteredTweet = classCorpusString.filter(_._2.contains(keyword))
      val noFilteredTweet =classCorpusString.filter(t => !(t._2.contains(keyword)))

      val yesTotalCount = yesFilteredTweet.length
      val noTotalCount = TWEET_TOTAL_COUNT - yesTotalCount

      val yesSide = yesFilteredTweet.map(list => (yesTotalCount, keyword, list._1, list._2))
      val noSide = noFilteredTweet.map(list => (noTotalCount, keyword, list._1, list._2))

      val yesYesCount = yesSide.filter(list => list._3.equals("p")).length
      val yesNoCount = yesTotalCount - yesYesCount

      val noYesCount = noSide.filter(list => list._3.equals("p")).length
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

    val keywordEntropy = sc.parallelize(final_data).sortBy(_._2, sort)

    keywordEntropy

  }

  def createMatrix(corpus: Array[(String, Seq[String])],
                    keywords: Array[String],
                    outputFilename: String): Unit = {

    // 1. corpus의 첫번째가 class, 나머지는 tweet content


    // 2. Matrix의 첫번째 Row는 각 키워드들
    val writer = new PrintWriter(new File(outputFilename))
    writer.write(keywords.mkString(",") + ",class\n")

    for (doc <- corpus) {
      for (key <- keywords) {
        val filtered = doc._2.filter(_.equals(key))
        if (!filtered.isEmpty) writer.write("1,")
        else writer.write("0,")
      }
      writer.write(doc._1 + "\n")
    }

    writer.close()

  }

}