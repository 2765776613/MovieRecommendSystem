package com.sl.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


object DataLoader {
  val MOVIE_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "linux:9200",
      "es.transportHosts" -> "linux:9300",
      "es.index" -> "recommender", // 相当于database
      "es.cluster.name" -> "es-cluster"
    )

    // TODO 1. 创建SparkSession对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO 2. 加载数据
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    // 将RDD封装到Movie中并且转换为DataFrame
    val movieDF: DataFrame = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim,
          attr(7).trim, attr(8).trim, attr(9).trim
        )
      }
    ).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF: DataFrame = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    // TODO 3. 保存数据到MongoDB 和 ES
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // 数据预处理，把movie对应的tag信息添加进去，加一列  tag1|tag2|...
    import org.apache.spark.sql.functions._

    val newTag: DataFrame = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")
    // newTag和movieDF做join 左外连接
    val movieWithTagsDF: DataFrame = movieDF.join(newTag, Seq("mid", "mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    storeDataInES(movieWithTagsDF)

    // TODO 4. 释放资源
    spark.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建MongoDB的连接
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果MongoDB中对应的数据库已有我需要的表，那么先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 将DF数据写入对应的MongoDB表中
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭 MongoDB 的连接
    mongoClient.close()
  }

  def storeDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // 新建es配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()
    // 新建es客户端
    val esClient = new PreBuiltTransportClient(settings)
    //需要将 TransportHosts 添加到 esClient 中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new
            InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }
    // 需要清除掉 ES 中遗留的数据
    if (esClient.admin().indices().exists(new
        IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    // 创建index
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    // 将movieDF写入到ES中
    movieDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
  *
  * @param uri MongoDB连接
  * @param db  MongoDB数据库
  */
case class MongoConfig(uri: String, db: String)

/**
  *
  * @param httpHosts      http主机列表，逗号分隔
  * @param transportHosts transport主机列表  用于集群内节点之间的内部通信
  * @param index          需要操作的索引
  * @param clustername    集群名称，默认
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)