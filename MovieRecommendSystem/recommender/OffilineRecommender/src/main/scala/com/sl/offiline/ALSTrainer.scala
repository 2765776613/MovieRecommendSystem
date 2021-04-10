package com.sl.offiline

import breeze.numerics.sqrt
import com.sl.offiline.OffilineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载评分数据
    val ratingRDD: RDD[Rating] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score))

    // 随机切分数据集，生成训练集和测试集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD: RDD[Rating] = splits(0)
    val testingRDD: RDD[Rating] = splits(1)

    // 模型参数选择，输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    spark.stop()
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]) = {
    val result: Array[(Int, Double, Double)] = for (rank <- Array(20, 50, 100); lambda <- Array(0.1, 0.001, 0.01))
      yield { // 每一次的中间结果都会保存下来
        // TODO 迭代次数设置大了，电脑跑不动！！！
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // 控制台打印输出
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val real: RDD[((Int, Int), Double)] = data.map(item => ((item.user, item.product), item.rating)) // 真实值
    val userMovies: RDD[(Int, Int)] = data.map(item => (item.user, item.product)) // 空矩阵
    val predictRating: RDD[Rating] = model.predict(userMovies) // 得到预测的评分矩阵
    // 预测值  跟真实值的数据格式保持一致
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))
    // 求解RMSE
    sqrt(
      // 以uid,mid作为外键，inner join
      real.join(predict).map {
        case ((uid, mid), (actual, pre)) => {
          val err: Double = actual - pre
          err * err
        }
      }.mean()
    )
  }
}
