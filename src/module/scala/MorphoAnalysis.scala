import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
  def makeRDDKeywordInTweet(inputPath :String, outputPath: String): Unit ={
    val tweetRDD = _sc.textFile(inputPath).map(_.split("\t")(1))
//    parseWord(tweet)
//    val words = MutableList[String]()
    val writer = new PrintWriter(new File(outputPath))

    for(tweet <- tweetRDD.collect()){
      //      filterStopWord(_mecab.parseWord(tweet).asScala.mkString(","))
      //      words += _mecab.parseWord(tweet).asScala.mkString(",")
      writer.write(_mecab.parseWord(tweet.replaceAll(_regexURL,"").replaceAll(_regexID,"")).asScala.mkString(","))
      writer.write("\n")
    }
    writer.close()

  }

  def parseTweet(rowData:String): String ={ rowData.split("\t")(_SPLIT_INDEX) }

  def parseWord(tweetRDD: RDD[String]): Unit ={

  }
  def isStopWord(word: String): Boolean = {
    val stopwordsBrod = fromFile("./dic/stopword").getLines().toString().split(",")
    stopwordsBrod.contains(word)
  }
}
