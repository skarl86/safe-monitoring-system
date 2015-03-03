import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by NCri on 15. 3. 3..
 */
class TFIDF {
  def run: Unit ={
    val conf = new SparkConf().setAppName("TF-IDF").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 2. 파일을 읽음.
    val documents = sc.textFile("./output/output.txt").map(_.split("\t").toSeq)

    // 3. 각 라인별 TF_IDF 수행.
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // 3-1. 각 키워드 Count
    val keywordCount: RDD[(String, Int)] = documents.flatMap(_.map((_, 1))).reduceByKey(_ + _)

    // 3-2. 각 키워드별 TF-IDF 매칭 작업.
    val keywordIndex = documents.map(seq => seq.map(str => (str, hashingTF.indexOf(str))))

    val keywordTfidf: RDD[(String, Double)] =
      keywordIndex.zip(tfidf).flatMap(t1 => t1._1.map(t2 => (t2._1, t1._2.toArray(t2._2))))

    // 각 document에 중복된 단어에 대한 average 계산. => 추후, average measurement가 아닌 다른 measurement를 찾아보자.
    val reduceKeyword: RDD[(String, Double)] =
      keywordTfidf.mapValues((_, 1))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .mapValues(x => x._1 / x._2)

    val keywordCountTfidf =
      keywordCount.join(reduceKeyword).sortBy(_._2._1, false)

    keywordCountTfidf.saveAsTextFile("output_spark")
  }
}
