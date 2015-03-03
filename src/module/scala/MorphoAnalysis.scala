import java.io.{File, PrintWriter}

import org.chasen.mecab.Tagger

import scala.io.Source._
import scala.collection.JavaConverters._

/**
 * Created by NCri on 15. 3. 3..
 */
class MorphoAnalysis {
  System.load(System.getProperty("java.library.path") + "/libMeCab.so")

  private val mecab = new MeCab()

  private val tagger = new Tagger("-d /usr/local/lib/mecab/dic/mecab-ko-dic")

  def makeKeywordInTweet(inputPath :String, outputPath: String): Unit ={
    val source = fromFile(inputPath)
    val lines = source.getLines()
    val writer = new PrintWriter(new File(outputPath))

    for(line <- lines){
      mecab.parseWord(line).asScala.foreach(word => writer.write(word + "\t"))
      writer.write("\n")
    }
    writer.close()
    source.close()
  }
}
