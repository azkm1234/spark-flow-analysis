import java.io.File
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingKmeans {
  val scaledFeatures = "scaledFeatures"
  val features = "features"
  val label = "label"
  val kmeansPrediction = "kmeansPrediction"
  val distanceToCentersVector = "distanceToCentersVector"
  val assembledFeatures = "assemblerFeatures"
  val indexedLabel = "indexedLabel"
  val rfPrediction = "rfPrediction"
  val predictedLabel = "predictedLabel"
  val pipeline2ModelPath = "data/pipeline2Model"
  val pipeline1ModelPath = "data/pipeline1Model"
  val numOfClusters = 23
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[3]").setAppName("KafkaStreamingKmeans")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topic = "testtopic"
    val data = getKafkaInputStream(topic,1,2,ssc)
    val pipeline1Model: PipelineModel = PipelineModel.load(pipeline1ModelPath)
    val pipeline2Model: PipelineModel  = PipelineModel.load(pipeline2ModelPath)

    data.foreachRDD(rdd => {
      val spark = SparkSessionSingleton.getInstance(conf)
      rddDataHandler(rdd,spark, pipeline1Model,  pipeline2Model)
    })
    ssc.start();
    ssc.awaitTermination();
  }
  def rddDataHandler(rdd:RDD[String], spark:SparkSession, pipeline1Model: PipelineModel, pipeline2Model: PipelineModel): Unit = {
    import spark.implicits._


    val data = rdd.flatMap{line => line.split("\n")}.map{line =>
      val strs = line.split(",")
      RecivedData(strs(0), strs(1), strs(2), strs(3).toDouble, strs(4), strs(5), strs(6), strs(7).toDouble, strs(8).toDouble, strs(9), strs(10).toDouble, strs(11).toDouble, strs(12).toDouble, strs(13).toDouble, strs(14).toDouble, strs(15).toDouble, strs(16).toDouble, strs(17).toDouble, strs(18).toDouble, strs(19).toDouble, strs(20).toDouble, strs(21).toDouble, strs(22).toDouble, strs(23).toDouble, strs(24).toDouble, strs(25).toDouble, strs(26).toDouble, strs(27).toDouble, strs(28).toDouble, strs(29).toDouble, strs(30).toDouble)
    }.toDF()
    handleData(data, spark, pipeline1Model,  pipeline2Model)
  }
  def handleData(data:DataFrame, spark:SparkSession, pipeline1Model: PipelineModel, pipeline2Model: PipelineModel): Unit = {
    if (data.count() > 0) {
      val pipeline1Result: DataFrame = pipeline1Model.transform(data)
        .select("timeStamp", "srcIp", "desIp", "scaledFeatures", "kmeansPrediction") //toDo 这里需要改,增加"timeStamp", "srcIp", "desIp"
      pipeline1Result.printSchema()
      val kMeansModel = pipeline1Model.stages(11).asInstanceOf[KMeansModel]
      val testData = convertData(spark, pipeline1Result, kMeansModel, scaledFeatures)
      testData.printSchema()

      val labels = pipeline1Model.stages(9).asInstanceOf[StringIndexerModel].labels

      val pipe2Result = pipeline2Model.transform(testData).select("timeStamp", "srcIp", "desIp", predictedLabel)
      pipe2Result.printSchema()
      val url = "jdbc:mysql://master:3306/hadoopdb"
      val talbeName = "testTable"
      val prop = new Properties()
      prop.setProperty("user", "root")
      prop.setProperty("password", "zjc1994")
      pipe2Result.write.jdbc(url, talbeName, prop)
    }
  }


  /**
    * 计算每一调数据到每一个center的距离的平方，
    * 并且作为特征值与原来的ScaledFeatures相结合
    */
  private def convertData(spark: SparkSession, data: DataFrame, model: KMeansModel, colName: String): DataFrame = {
    val clusterCenters = model.clusterCenters
    val appendClusterCenter = udf((features: Vector) => {
      val r = clusterCenters.toArray.map { v1 =>
        Vectors.sqdist(v1, features)
      }
      Vectors.dense(r)
    })
    data.withColumn(distanceToCentersVector, appendClusterCenter(col(colName)))
  }

  def getKafkaInputStream(topic: String, numRecivers: Int,
                          partition: Int, ssc: StreamingContext): DStream[String] = {
    val kafkaParams = Map(
      "zookeeper.connect" -> "master:2181,slave1:2181,slave2:2181",
      "zookeeper.session.timeout.ms" -> "500",
      "auto.commit.interval.ms" -> "1000",
      "zookeeper.sync.time.ms" -> "250",
      "auto.offset.reset" -> "smallest",
      ("zookeeper.connection.timeout.ms", "30000"),
      ("fetch.message.max.bytes", (1024 * 1024 * 50).toString),
      ("group.id", "testgroup")
    )
    val topics = Map(topic -> partition / numRecivers)

    val kafkaDstreams: Seq[DStream[String]] = (1 to numRecivers).map { _ =>
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
        kafkaParams,
        topics,
        StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    }
    ssc.union(kafkaDstreams)
  }
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
  case class RecivedData(timeStamp:String, srcIp:String, desIp:String, duration:Double, protocol_type:String, service:String, flag:String, src_bytes:Double, dst_bytes:Double, land:String, wrong_fragment:Double, urgent:Double, count:Double, srv_count:Double, serror_rate:Double, srv_serror_rate:Double, rerror_rate:Double, srv_rerror_rate:Double, same_srv_rate:Double, diff_srv_rate:Double, srv_diff_host_rate:Double, dst_host_count:Double, dst_host_srv_count:Double, dst_host_same_srv_rate:Double, dst_host_diff_srv_rate:Double, dst_host_same_src_port_rate:Double, dst_host_srv_diff_host_rate:Double, dst_host_serror_rate:Double, dst_host_srv_serror_rate:Double, dst_host_rerror_rate:Double, dst_host_srv_rerror_rate:Double)

}
