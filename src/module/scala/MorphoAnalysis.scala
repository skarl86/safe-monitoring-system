import java.io.{File, PrintWriter}
import java.sql.{ResultSet, DriverManager}

import breeze.storage.Storage
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable._
import scala.io.Source._
import scala.collection.JavaConverters._

/**
 * Created by NCri on 15. 3. 3..
 */

class MorphoAnalysis(sc:SparkContext) {

  System.load(System.getProperty("java.library.path") + "/libMeCab.so")
  private val _regexURL = "http://(([a-zA-Z][-a-zA-Z0-9]*([.][a-zA-Z][-a-zA-Z0-9]*){0,3})||([0-9]{1,3}([.][0-9]{1,3}){3}))/[a-zA-Z0-9]*"
  private val _regexID = "@[a-zA-Z0-9_:]*"
  private val _mecab = new MeCab()
  private val _sc = sc
  private val _db = "jdbc:mysql://218.54.47.24:3306/tweet?user=root&password=tkfkdgo1_"

  def writeFileForTextPerLine(path:String, textPerLine:String): Unit ={
    val writer = new PrintWriter(new File(path))
    writer.write(textPerLine)
    writer.write("\n")
    writer.close()
  }

  def writeFileForTextPerLine(path:String, textPerLines:List[String]): Unit ={
    val writer = new PrintWriter(new File(path))
    writer.write(textPerLines.mkString("\n"))
    writer.close()
  }
  def makeKeywordInTweet(inputPath :String, outputPath: String): Unit ={
    val source = fromFile(inputPath)
    val lines = source.getLines()
    val writer = new PrintWriter(new File(outputPath))

    for(line <- lines){
      _mecab.parseWord(line).asScala.foreach(word => writer.write(word + ","))
      writer.write("\n")
    }
    writer.close()
    source.close()
  }
  // domain = > 야구(1), 축구(2), 건강(3)
  def makeRDDKeywordInTweet(domain : Int): Unit = {
    val tweetRDD = getTweetDataFrom(domain)
    for (tweet <- tweetRDD.collect()) {
      val wordList = ngram2(2, _mecab.parseWord(tweet._2.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)
      insertKeywordToDB(tweet._1,wordList)
    }
  }
  def makeRDDKeywordInTweet(inputPath :String, outputPath: String): Unit = {
    val tweetRDD = getTweetDataFrom(inputPath)
    for (tweet <- tweetRDD.collect()) {
      val wordList = ngram2(2, _mecab.parseWord(tweet.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)

      writeFileForTextPerLine(outputPath, wordList.mkString(","))
    }
  }

  def insertKeywordToDB(tweetID:Int, words:List[String] ): Unit ={
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    try {
      val prep = conn.prepareStatement("INSERT INTO keyword (tweetid, keyword) VALUES (?, ?) ")
      prep.setInt(1, tweetID)
      prep.setString(2, words.mkString(","))
      prep.executeUpdate
    }
    finally {
      conn.close
    }
  }
  def getTweetDataFrom(domain : Int): RDD[(Int, String)] = {

    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement()
    var tweetData:ListBuffer[(Int, String)] = new ListBuffer()

    // do database insert
    try {
      val resultSet = statement.executeQuery("SELECT tweetid, text FROM marktweettable where domain = " + domain)

      while(resultSet.next()){
        val tweetId = resultSet.getInt("tweetid")
        val text = resultSet.getString("text")

        val tuple = (tweetId, text)
        tweetData += tuple
      }
    } catch{
      case e => e.printStackTrace
    }
    conn.close

    _sc.parallelize(tweetData.toList)
  }
  def getTweetDataFrom(path : String): RDD[String] = {
    _sc.textFile(path).map(_.split("\t")(1))
  }

  def ngram2(n: Int, words: List[String]): List[String] = {
    val ngrams = (for( i <- 1 to n) yield words.sliding(i).map(p => p.toList)).flatMap(x => x)
    var newWords = new ListBuffer[String]()
    for(cardinate <- ngrams){
      newWords += (cardinate.mkString)
    }
    newWords.toList
  }
  def ngram(n : Int, words: List[String]): List[String] = {
    val categoryKeywordsRDD = _sc.textFile("./dic/keywords.txt").map(_.split(","))
    val ngrams = (for( i <- n to n) yield words.sliding(i).map(p => p.toList)).flatMap(x => x)
    var originalWords = new ListBuffer[String]()
    var newWords = new ListBuffer[String]()

    // 예:)  words => 나 노로 바이러스
    //      originalWords => 나 노로 바이러스
    //      newWords => 노로바이러스
    //      originalWords => 나  (노로, 바이러스는 삭제시킨다.)
    //      newWords ++ originalWords => 나 노로바이러스.
    originalWords = originalWords ++ words

    // cardinate = List(노로, 바이러스)
    for(cardinate <- ngrams){
      val category = categoryKeywordsRDD.collect()(0)
      val nKeyword = cardinate.mkString
      if(category.contains(nKeyword)){
        for(nWord <- cardinate){
          if(originalWords.contains(nWord)){
            originalWords -= nWord
          }
        }
        newWords += cardinate.mkString
      }
    }
    newWords ++= originalWords
    newWords.toList
  }
}
