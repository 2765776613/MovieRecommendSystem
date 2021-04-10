package com.sl.offiline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object OffilineRecommender {
  // 定义表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // TODO 1. 创建SparkSession对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OffilineRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO 2. 从MongoDB加载数据
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache() // 数据缓存

    // 从rating数据中提取所有的uid和mid，并且去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // TODO 3. 创建训练集
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    // TODO 4. 训练模型
    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是 ALS 的正则化参数
    val (rank, iterations, lambda) = (50, 5, 0.01)
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    // TODO 5. 计算用户推荐矩阵
    // 建立空的评分矩阵
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)
    // 调用model的predict方法预测评分
    val preRatings: RDD[Rating] = model.predict(userMovies)
    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0) // 过滤出评分大于0的项
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        // 取评分排名前20的推荐电影
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // TODO 6. 计算电影相似度矩阵
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    // 对所有电影两两计算他们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 } // 过滤掉自己跟自己的匹配
      .map {
      case (a, b) => {
        val simScore = this.consinSim(a._2, b._2) // 求余弦相似度
        (a._1, (b._1, simScore))
      }
    }.filter(_._2._2 > 0.6) // 过滤出相似度大于0.6的电影
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  //计算两个电影之间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}

// 基于评分数据的LFM，只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

// 用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 电影相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
