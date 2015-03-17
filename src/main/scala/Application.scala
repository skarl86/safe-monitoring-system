import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by NCri on 15. 3. 3..
 */
object Application {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Safe-Monitoring-System").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 1. 기본적인 Class 설정
    val morpho = new MorphoAnalysis(sc)
    val featureExtractor = new FeatureExtractor(sc)
    val testDriver = new TestDriver(sc, morpho, featureExtractor)

    // 2. Matrix 생성 모듈
    testDriver.run("input/data.txt", "matrix.csv", null, "keyword/keywords.txt", 1000, 200, "tf")
    // 2-2. keyword list 가 존재할 경우 (test_set)
//    testDriver.run("input/data.txt", "matrix.csv", "keyword/keywords.txt", null, 0, 0, "tf")

  }
}
