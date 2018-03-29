import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadNames {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("DirectKafWordCount")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val names: Array[String] = spark.read.textFile("data/name.txt").collect()
    println(names.length)
  }
}
