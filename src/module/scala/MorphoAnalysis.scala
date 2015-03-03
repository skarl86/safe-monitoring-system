import java.io.{File, PrintWriter}
import java.sql.{ResultSet, DriverManager}

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

class MorphoAnalysis(conf:SparkConf, sc:SparkContext) {
  System.load(System.getProperty("java.library.path") + "/libMeCab.so")
  private val _wordClass: String = "(.*NNG.*|.*NNP.*|.*NNB.*|.*NR.*|.*NP.*|.*SL.*)"
  private val _regexURL = "http://(([a-zA-Z][-a-zA-Z0-9]*([.][a-zA-Z][-a-zA-Z0-9]*){0,3})||([0-9]{1,3}([.][0-9]{1,3}){3}))/[a-zA-Z0-9]*"
  private val _regexID = "@[a-zA-Z0-9_:]*"
  private val _mecab = new MeCab()
  private val _conf = conf
  private val _sc = sc
  private val _SPLIT_INDEX = 1
  private val _db = "jdbc:mysql://218.54.47.24:3306/tweet?user=root&password=tkfkdgo1_"
  private val _delimeter:String = ","

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
  def makeRDDKeywordInTweet(inputPath :String, outputPath: String): Unit = {
    val tweetRDD = getTweetDataFromDB
//    val tweetRDD = getTweetDataFromFile(inputPath)//_sc.textFile(inputPath).map(_.split("\t")(2))
//
    val writer = new PrintWriter(new File(outputPath))
//
    for (tweet <- tweetRDD.collect()) {
      //      filterStopWord(_mecab.parseWord(tweet).asScala.mkString(","))
      //      words += _mecab.parseWord(tweet).asScala.mkString(",")

      //      writer.write(_mecab.parseWord(tweet.replaceAll(_regexURL,"").replaceAll(_regexID,"")).asScala.mkString(","))
      //      writer.write("\n")
      val wordList = ngram(2, _mecab.parseWord(tweet._2.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)

//      insertKeywordToDB(tweet._1,wordList)

      writer.write(wordList.mkString(","))
      writer.write("\n")
    }
    writer.close()

  }
  def insertKeywordToDB(tweetID:Int, words:List[String] ): Unit ={
//    println("(" + tweetID + ", " + words.mkString(","))
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    // do database insert
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
  def getTweetDataFromFile(path : String): RDD[String] ={
    _sc.textFile(path).map(_.split("\t")(2))
  }

  def getStopWordFromDB(): RDD[String] ={
    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement()
    var stopWords:ListBuffer[String] = new ListBuffer()

    // do database insert
    try {
      val resultSet = statement.executeQuery("SELECT stopword FROM stopwordtable")
      while(resultSet.next()){
//        stopWords += resultSet.getString("stopword")
        println(resultSet.getString("stopword"))
      }
    } catch{
      case e => e.printStackTrace
    }

    _sc.parallelize(stopWords.toList)
  }
  def getTweetDataFromDB(): RDD[(Int, String)] ={
    // DB연동

    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement()
    var tweetData:ListBuffer[(Int, String)] = new ListBuffer()

    // do database insert
    try {
      val resultSet = statement.executeQuery("SELECT tweetid, text FROM marktweettable")
      while(resultSet.next()){
        val tweetId = resultSet.getInt("tweetid")
        val text = resultSet.getString("text")

//        println("ID = " + tweetId + " // text = " + text)
        val tuple = (tweetId, text)
        tweetData += tuple
      }
    } catch{
      case e => e.printStackTrace
    }
    conn.close

    _sc.parallelize(tweetData.toList)
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

  def parseTweet(rowData:String): String ={ rowData.split("\t")(_SPLIT_INDEX) }

  def parseWord(tweetRDD: RDD[String]): Unit ={

  }
  def isStopWord(word: String): Boolean = {
    val stopwordsBrod = fromFile("./dic/stopword").getLines().toString().split(",")
    stopwordsBrod.contains(word)
  }
}
