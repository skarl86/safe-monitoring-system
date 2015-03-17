/**
 * Created by NCri on 15. 3. 17..
 */

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._
import scala.io.Source._

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser

class TweetJsonParser {
  def parserTweetRowData(jsonStr:String): String ={
    val jelement: JsonElement = new JsonParser().parse(jsonStr)
    val jobject: JsonObject = jelement.getAsJsonObject
    val tweetText = jobject.get("text").toString.replaceAll("\"","")
    val tweetid = jobject.get("_id").toString.replaceAll("\"","")
    val time = jobject.get("created_at")//.getAsString.substring(0,14) + "00:00"
    val timestamp = jobject.get("msg_create_at_forlong")

    List(tweetid, time, timestamp, tweetText).mkString("\t")
  }
}
