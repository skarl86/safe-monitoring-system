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
  private val _mecab = new MeCab()
  private val _conf = conf
  private val _sc = sc

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
  def makeRDDKeywordInTweet(inputPath :String): Unit ={


    val tweet = _sc.textFile(inputPath).map(_.split("\t")(2))
//    tweet.collect().foreach(println)
//    println(parseWord(tweet))
  }

  def parseTweet(rowData:String): String ={ rowData.split("\t")(2) }

  def parseWord(tweetRDD: RDD[String]): Unit ={
    val words = MutableList[String]()

    for(tweet <- tweetRDD.collect()){
      words += _mecab.parseWord(tweet).asScala.mkString(",")
    }
    val stopword = fromFile("./dic/stopword").getLines().toString().split(",")
    val filteredWord = words.filter(stopword.contains(_))
    filteredWord.foreach(println)
  }

  def isStopWord(word: String): Boolean = {
    val stopwordsBrod = fromFile("./dic/stopword").getLines().toString().split(",")
    stopwordsBrod.contains(word)
  }
}
