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

    // do database insert
    try {
      val resultSet = statement.executeQuery(
        "SELECT keyword FROM keyword")
      while(resultSet.next()){
        keywords += resultSet.getString("keyword")
//        println(resultSet.getString("keyword"))
      }
    } catch{
      case e => e.printStackTrace
    }

    sc.parallelize(keywords.toList)
  }

  def termFrequency(corpus: RDD[Seq[String]],
                    desending: Boolean = false) = {

    val keywordCount: RDD[(String, Int)] =
      corpus.flatMap(_.map((_, 1)))
        .reduceByKey(_ + _)

    val static: RDD[Int] = keywordCount.map(_._2)

    val mean = static.mean()
    val range = static.max() - static.min()
    // Mean Normalization 적용
    val normalKeywordCount = keywordCount.map(t => (t._1, (t._2 - mean) / range))

    normalKeywordCount.sortBy(_._2, desending)

  }

  def tfidf(corpus: RDD[Seq[String]],
            method: String = "average",
            sort: String = "count",
            desending: Boolean = false
            ) = {

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(corpus)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // 1. 각 키워드 Count
    val keywordCount: RDD[(String, Int)] = corpus.flatMap(_.map((_, 1))).reduceByKey(_ + _)

    // IDF
//    val keywordIdf = corpus.flatMap(seq => seq.map(str => (str, idf.idf(hashingTF.indexOf(str)))))
//
//    val keywordIdf2 =
//      keywordIdf.mapValues((_, 1))
//      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//      .mapValues(x => x._1 / x._2)

    // 2. 각 키워드별 TF-IDF 매칭 작업.
    val keywordIndex = corpus.map(seq => seq.map(str => (str, hashingTF.indexOf(str))))

    val keywordTfidf: RDD[(String, Double)] =
      keywordIndex.zip(tfidf).flatMap(t1 => t1._1.map(t2 => (t2._1, t1._2.toArray(t2._2))))

    // Vector 만들기
//    matrix match {
//      case true =>
//        val keyword: Array[String] = keywordCount.map(_._1).collect()
//        val keywordTfidfInDoc: Array[Seq[(String, Double)]] =
//          keywordIndex.zip(tfidf).map(t1 => t1._1.map(t2 => (t2._1, t1._2.toArray(t2._2)))).collect()
//
//
//        val writer = new PrintWriter(new File("matrix.txt" ))
//        writer.write(keyword.mkString(",") + "\n")
//
//        for (doc <- keywordTfidfInDoc) {
//          for (key <- keyword) {
//            val filtered = doc.filter(_._1.equals(key)).map(_._2)
//            if (!filtered.isEmpty) writer.write(filtered(0).toString() + ",")
//            else writer.write("-1.0,")
//          }
//          writer.write("\n")
//        }
//
//        writer.close()
//    }

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
    var keywordCountTfidf: RDD[(String, (Int, Double))] = null

//    val keywordCountTfidf = keywordCount.join(reduceKeyword).join(keywordIdf2).sortBy(_._2._2, desending)
    sort match  {
      case "count" =>
        keywordCountTfidf =
          keywordCount.join(reduceKeyword).sortBy(_._2._1, desending)

      case "tfidf" =>
        keywordCountTfidf =
          keywordCount.join(reduceKeyword).sortBy(_._2._2, desending)
    }

    keywordCountTfidf

  }

  def entropy(classCorpus: RDD[(String, String)],
              keywords: RDD[String],
              desending: Boolean = false,
              matrix: Boolean = false) = {

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

    val keywordEntropy = sc.parallelize(final_data).sortBy(_._2, desending)

    matrix match {
      case true => {
        val documents: Array[(String, Seq[String])] =
          classCorpus.map(t => (t._1, t._2.split(" ").toSeq)).collect()

        val keyword: Array[String] = keywordEntropy.map(_._1).take(200)

        val writer = new PrintWriter(new File("matrix_entropy.csv"))
        writer.write(keyword.mkString(",") + ",class\n")

        for (doc <- documents) {
          for (key <- keyword) {
            val filtered = doc._2.filter(_.equals(key))
            if (!filtered.isEmpty) writer.write("1,")
            else writer.write("0,")
          }
          writer.write(doc._1 + "\n")
        }

        writer.close()
      }
      case false => {

      }
    }

    keywordEntropy

  }

  def entropyMatrix(classCorpus: RDD[(String, String)],
                    keywords: RDD[String]): Unit = {

    val documents: Array[(String, Seq[String])] =
      classCorpus.map(t => (t._1, t._2.split(" ").toSeq)).collect()

    val keyword: Array[String] = keywords.collect()

    val writer = new PrintWriter(new File("test_matrix_entropy.csv"))
    writer.write(keyword.mkString(",") + ",class\n")

    for (doc <- documents) {
      for (key <- keyword) {
        val filtered = doc._2.filter(_.equals(key))
        if (!filtered.isEmpty) writer.write("1,")
        else writer.write("0,")
      }
      writer.write(doc._1 + "\n")
    }

    writer.close()

  }

  def apriori(inputPath: String,
              maxIterations: Int,
              minSup: Int,
              outputPath: String) = {

    val documentIdDelimiter = "\t"
    val documentItemDelimiter = " "

    var k = 1
    var hasConverged = false

    // Step1
    val documents = sc.textFile(inputPath).map { line =>
      val lineIndex = line.indexOf(documentIdDelimiter)
      val key = line.substring(0, lineIndex)
      val value = line.substring(lineIndex + 1, line.length)
      value.split(documentItemDelimiter).distinct.mkString(documentItemDelimiter)
    }

    var previousRules: Broadcast[Array[String]] = null

    def findCandidates(documents: RDD[String],
                       prevRules: Broadcast[Array[String]],
                       k: Int,
                       minSup: Int): RDD[String] = {
      documents.flatMap { items =>
        var cItems1: Array[Int] = items.split(documentItemDelimiter).map(_.toInt).sorted.toArray
        var combGen1 = new CombinationGenerator()
        var combGen2 = new CombinationGenerator()

        var candidates = scala.collection.mutable.ListBuffer.empty[(String, Int)]
        combGen1.reset(k, cItems1)

        while (combGen1.hasNext()) {
          var cItems2 = combGen1.next()
          var valid = true
          if (k > 1) {
            combGen2.reset(k-1, cItems2)
            while (combGen2.hasNext && valid) {
              valid = prevRules.value.contains(java.util.Arrays.toString(combGen2.next()))
            }
          }
          if (valid) {
            candidates += Tuple2(java.util.Arrays.toString(cItems2), 1)
          }
        }
        candidates
      }.reduceByKey(_+_).filter(_._2 >= minSup).map { case (itemset, _) => itemset }
    }

    while(k < maxIterations && !hasConverged) {
      printf("Starting Iteration %s\n", k)
      // Step2
      var supportedRules = findCandidates(documents, previousRules, k, minSup)
      var tempPrevRules = supportedRules.collect()
      var ruleCount = tempPrevRules.length
      if (0 == ruleCount) {
        hasConverged = true
      } else {
        previousRules = sc.broadcast(tempPrevRules)
        supportedRules.coalesce(1).saveAsTextFile(outputPath + "/" + k)
        k += 1
      }
    }

    printf("Converged at Iteration %s\n", k)

  }

}
