import java.io.{IOException, File, PrintWriter}
import java.sql.{ResultSet, DriverManager}
import java.sql.Date

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.sql.PreparedStatement
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
  private val _db = "jdbc:mysql://218.54.47.24:3306/tweetdata?user=tweetdatauser&password=tweetdatauser"

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
  def inputRowTweetDataFromPreprocessedData(tweetRawDirPath:String): Unit ={
    classOf[com.mysql.jdbc.Driver]
    val dir: File = new File(tweetRawDirPath)
    val tweetParser = new TweetJsonParser
    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    val fileList: Array[File] = dir.listFiles

    try { {
      var i: Int = 0
      var fileCount: Long = 0
      var rowCount: Long = 0

      while (i < fileList.length) {
        {
          val file: File = fileList(i)

          if (file.isFile) {
            if (file.getName.startsWith("pre") && (file.length > 0L)) {
              val lines = fromFile(file).getLines()
              fileCount += 1
              for(line <- lines){
                val Array(id:String, lat:String, long:String, time:String, timestamp:String, text:String) = line.split("\t")
                val wordList = ngram2(2, _mecab.parseWord(text.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)
                // 도구 프로그램에서 예외처리가 필요함.
                val documentStr = "," + wordList.mkString(",") + ","
                var prep:PreparedStatement = null

                rowCount += 1

                lat match{
                  case "None" =>{
//                    println(List(rowCount,fileCount).mkString(" / ") + ": Not Have GEO")
                    val query = "INSERT INTO RawTable (tweetid, createdAt, text, document) VALUES (?, ?, ?, ?)"
                    prep = conn.prepareStatement(query)
                  }
                  case lat : String =>{
//                    println(List(rowCount,fileCount).mkString(" / ") + ": Have GEO : (" + List(id, lat, long).mkString(",") + ")")
                    val query = "INSERT INTO RawTable (tweetid, createdAt, text, document, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)"
                    prep = conn.prepareStatement(query)
                    prep.setDouble(5, lat.toDouble)
                    prep.setDouble(6, long.toDouble)

                  }
                }
                prep.setLong(1, id.toLong)
                prep.setDate(2, getDate(timestamp.toLong))
                prep.setString(3, text)
                prep.setString(4, documentStr)

                prep.executeUpdate
              }
            }
          }
          else if (file.isDirectory) {
            //            subDirList(file.getCanonicalPath.toString)
          }
        }
        println("Input Complete(" + (i+1) + "/" + fileList.length + ")")
        ({
          i += 1; i - 1
        })
      }
    }
    }
    catch {
      case e: IOException => {
      }
    }
    finally {
      conn.close()
    }
  }

  def inputRowTweetDataFromJson(jsonPath :String): Unit = {
    classOf[com.mysql.jdbc.Driver]
    val source = fromFile(jsonPath).getLines()
    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    val tweetParser = new TweetJsonParser

    // TEST CODE
//    val writer = new PrintWriter(new File("output/jsonOutput.txt"))

    try{
      for(line <- source){
        // Tweeter JSON에서 id, time, text를 가져온다.
        // time 에서 얻어오는 날짜 정보는 미국 현지 시간을 기준으로 측정된 값. ( EEE, d MMM HH:mm:ss Z yyyy 포멧의 String)
        // timestamp값은 한국 현지 시간을 기준으로 측정된 값. (Long 값의 String)
        val Array(id:String, lat:String, long:String, time:String, timestamp:String, text:String) = tweetParser.parserTweetRowData(line).split("\t")
        val wordList = ngram2(2, _mecab.parseWord(text.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)
        // 도구 프로그램에서 예외처리가 필요함.
        val documentStr = "," + wordList.mkString(",") + ","
        var prep:PreparedStatement = null

        lat match{
          case lat : String =>{
            val query = "INSERT INTO RawTable (tweetid, createdAt, text, document, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)"
            println(query)
            prep = conn.prepareStatement(query)
            prep.setDouble(5, lat.toDouble)
            prep.setDouble(6, long.toDouble)

          }
          case null =>{
            val query = "INSERT INTO RawTable (tweetid, createdAt, text, document) VALUES (?, ?, ?, ?)"
            prep = conn.prepareStatement(query)
          }
        }
        prep.setLong(1, id.toLong)
        prep.setDate(2, getDate(timestamp.toLong))
        prep.setString(3, text)
        prep.setString(4, documentStr)

        prep.executeUpdate
//        println(List(id.toLong, getDate(timestamp.toLong), text, documentStr).mkString("\t"))
//        writer.write(List(id, getTime(timestamp.toLong), text,documentStr).mkString("\t"))
//        writer.write("\n")
      }
    }
    finally{
//      writer.close()
      conn.close()
    }

  }
  // domain = > 야구(1), 축구(2), 건강(3)
  def makeRDDKeywordInTweet(domain : Int): Unit = {
    val tweetRDD = getTweetDataFrom(domain)
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(_db)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    try {
      for (tweet <- tweetRDD.collect()) {
        val wordList = ngram2(2, _mecab.parseWord(tweet._2.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)

        val prep = conn.prepareStatement("INSERT INTO keyword (tweetid, keyword) VALUES (?, ?) ")

        prep.setInt(1, tweet._1)
        prep.setString(2, "," + wordList.mkString(",") + ",")
        prep.executeUpdate

      }
    }
    finally {
      conn.close
    }
  }
  def makeRDDKeywordInTweet(inputPath :String, outputPath: String): Unit = {
    val tweetRDD = getTweetDataFrom(inputPath)
    val writer = new PrintWriter(new File(outputPath))
    for (tweet <- tweetRDD.collect()) {
      val wordList = ngram2(2, _mecab.parseWord(tweet.replaceAll(_regexURL, "").replaceAll(_regexID, "")).asScala.toList)

      writer.write(wordList.mkString(","))
      writer.write("\n")
//      writeFileForTextPerLine(outputPath, wordList.mkString(","))
    }
    writer.close()

  }
  /*
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
  */
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
    if(words.size == 1){
      words
    }else{
      val ngrams = (for( i <- 1 to n) yield words.sliding(i).map(p => p.toList)).flatMap(x => x)
      var newWords = new ListBuffer[String]()
      for(cardinate <- ngrams){
        newWords += (cardinate.mkString)
      }
      newWords.toList
    }


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

  def getDate(timestamp:Long): java.sql.Date ={
    val ts = new Timestamp(timestamp)
    val date = new java.sql.Date(ts.getTime())
    date
  }
}
