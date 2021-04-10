package com.sl.statistics

import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StatisticsRecommender {
  // 定义表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies" // 历史热门
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" // 最近热门
  val AVERAGE_MOVIES = "AverageMovies" // 平均评分
  val GENRES_TOP_MOVIES = "GenresTopMovies" // 每个类别的热门电影top10

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // TODO 1. 创建SparkSession对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO 2. 从MongoDB加载数据
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    val movieDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // TODO 3. 不同的统计推荐结果
    // 创建名为ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO 3.1 历史热门统计，历史评分数据最多  mid,count
    val rateMoreMoviesDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid")
    // 把结果写入对应的MongoDB表中
    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // TODO 3.2 近期热门统计，按照"yyyyMM"格式选取最近的评分数据，统计评分个数  mid, count, yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf，把时间戳转换成年月格式  new Date(毫秒) x是以秒为单位的，所以必须乘以1000
    // 但是乘以1000以后，int类型已经装不下了，所以要用1000L，但是为了统一，最后还是得转为int
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 对时间戳进行处理
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    // 从ratingOfMonth中查找电影在各个月份的评分统计，
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count ,yearmonth from ratingOfMonth group by yearmonth,mid order by yearmonth,count desc")
    // 把结果写入对应的MongoDB表中
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // TODO 3.3 优质电影统计，统计电影的平均评分  mid, score
    val averageMoviesDF = spark.sql("select mid,avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // TODO 3.4 各类别电影Top统计
    // 定义所有类别
    val genres: List[String] = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    // 把平均评分加入到movie表里，加一列 inner join
    val movieWithScore: DataFrame = movieDF.join(averageMoviesDF, "mid")

    // 为了做笛卡尔积，先将genres转为RDD
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)

    // 计算类别top10，首先对类别和电影做笛卡尔积
    val genresTopMoviesDF: DataFrame = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        // 找出movie的字段genres值包含当前类别的那些电影
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map {
        // 拿出movie中有用的字段
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }.groupByKey() // 聚合
      .map {
      case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
    }.toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐样例类
case class Recommendation(mid: Int, score: Double)

// 定义电影类别top10推荐样例类
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])