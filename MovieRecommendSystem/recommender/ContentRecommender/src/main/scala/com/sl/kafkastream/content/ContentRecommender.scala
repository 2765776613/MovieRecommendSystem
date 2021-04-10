package com.sl.kafkastream.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

object ContentRecommender {
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // TODO 1. 创建SparkSession对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO 2. 从MongoDB加载数据
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val movieTagsDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      // 因为分词器默认是按照空格进行分词，所以这里提前进行转换
      .map(x => (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c)))
      .toDF("mid", "name", "genres")
      .cache()

    // TODO 3. 从内容信息中提取电影特征向量  核心部分

    // TODO 3.1 创建一个分词器，默认按照空格分词
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    // 用分词器对原始数据做转换，生成新的一列words
    val wordsData: DataFrame = tokenizer.transform(movieTagsDF)

    // TODO 3.2 引入HashingTF工具，可以把一个词语序列转换成对应的词频
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    // TODO 3.3 引入IDF工具，可以得到idf模型
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练idf模型，得到每个词的逆文档频率
    val idfModel: IDFModel = idf.fit(featurizedData)
    // 用模型对原数据进行处理，得到文档中每个词的td-idf
    val rescaledData: DataFrame = idfModel.transform(featurizedData)

    // TODO 3.4 把特征转为DoubleMatrix类型便于后面计算相似度
    // 从计算得到的 rescaledData 中提取特征向量
    val movieFeatures = rescaledData.map {
      case row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    }.rdd
      .map(x => {
        (x._1, new DoubleMatrix(x._2))
      })

    // TODO 4. 计算电影相似度矩阵
    // 对所有电影两两计算他们的相似度，先做笛卡尔积
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
      .option("collection", CONTENT_MOVIE_RECS)
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

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

// 基于电影内容信息提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])