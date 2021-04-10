package com.sl.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

//object StreamingRecommender {
//  val MAX_USER_RATINGS_NUM = 20 // 选择的最大用户评分的个数  K  -> RK
//  val MAX_SIM_MOVIES_NUM = 20 // 选择的最大相似电影的个数  N  -> S
//  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
//  val MONGODB_RATING_COLLECTION = "Rating"
//  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
//
//
//  def main(args: Array[String]): Unit = {
//    val config = Map(
//      "spark.cores" -> "local[*]",
//      "mongo.uri" -> "mongodb://linux:27017/recommender",
//      "mongo.db" -> "recommender",
//      "kafka.topic" -> "recommender"
//    )
//
//    // 创建SparkSession对象
//    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
//    sparkConf.set("spark.driver.allowMultipleContexts", "true")
//    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
//    import spark.implicits._
//    val sc: SparkContext = spark.sparkContext
//    // 获取streaming context   批处理时间为2s
//    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))
//    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
//
//    // 加载电影的相似度矩阵数据，把它广播出去broadcast
//    val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
//      .option("uri", config("mongo.uri"))
//      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
//      .format("com.mongodb.spark.sql")
//      .load()
//      .as[MovieRecs]
//      .rdd
//      .map {
//        movieRecs => (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
//      }.collectAsMap()
//    val simMovieMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simMovieMatrix)
//
//    // 定义kafka连接参数
//    val kafkaParam = Map(
//      "bootstrap.servers" -> "linux:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "recommender",
//      "auto.offset.reset" -> "latest"
//    )
//    // 通过kafka创建一个DStream
//    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam))
//
//    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
//    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
//      msg => {
//        val attr: Array[String] = msg.value().split("\\|")
//        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
//      }
//    }
//    ratingStream.print()
//    // 继续做流式处理，核心实时算法部分
////    ratingStream.foreachRDD {
////      rdds => {
////        rdds.foreach {
////          case (uid, mid, score, timestamp) => {
////            println("rating data coming!>>>>>>>>>>>>>>>>>>>")
////            // 1. 从redis里获取当前用户最近的K次评分，保存成Array[(mid,score)]
////            val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
////
////            // 2. 从相似度矩阵中取出当前电影的最相似的K个电影，作为备选列表，Array[mid]
////            // 传入uid，是为了过滤掉用户看过的电影，那么需要查MongoDB,可以通过隐式传参
////            val candidateMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)
////
////            // 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid,score)]
////            val streamRecs: Array[(Int, Double)] = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
////
////            // 4. 把推荐数据保存到MongoDB中
////            saveDataToMongoDB(uid, streamRecs)
////          }
////        }
////      }
////    }
//
//
//    // 启动Streaming程序
//    ssc.start()
//    println(">>>>>>>>>>>>>>>>>>streaming started!")
//    ssc.awaitTermination()
//  }
//
//  // redis操作返回的是java类，为了用map操作需要引入转换类
//  import scala.collection.JavaConversions._
//
//  // 获取当前用户最近的K次电影评分
//  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
//    // 从redis读取数据，用户评分数据保存在uid:UID为key的队列里，value是MID:SCORE
//    jedis.lrange("uid:" + uid, 0, num - 1)
//      .map { // 具体每个评分又是以冒号分割的两个值
//        item => {
//          val attr: Array[String] = item.split("\\:")
//          (attr(0).trim.toInt, attr(1).trim.toDouble)
//        }
//      }.toArray
//  }
//
//  // 获取当前电影最相似的K个电影
//  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: collection.Map[Int, Map[Int, Double]])
//                     (implicit mongoConfig: MongoConfig): Array[Int] = {
//    // 1. 从相似度矩阵中拿到所有相似的电影
//    val allSimMovies: Array[(Int, Double)] = simMovies(mid).toArray
//
//    // 2. 从MongoDB中查询用户已经看过的电影
//    val ratingExist: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
//      .find(MongoDBObject("uid" -> uid)).toArray
//      .map {
//        item =>
//          item.get("mid").toString.toInt
//      }
//
//    // 3. 把看过的过滤，得到输出列表
//    allSimMovies.filter(x => !ratingExist.contains(x._1))
//      .sortWith(_._2 > _._2)
//      .take(num).map(_._1)
//  }
//
//
//  // 电影推荐优先级计算
//  def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)],
//                         simMovies: collection.Map[Int, Map[Int, Double]]): Array[(Int, Double)] = {
//    // 定义一个ArrayBuffer 用于保存每一个备选电影的基础得分
//    val scores = ArrayBuffer[(Int, Double)]()
//
//    // 定义一个HashMap 保存每一个备选电影的增强减弱因子
//    val increMap = HashMap[Int, Int]()
//    val decreMap = HashMap[Int, Int]()
//
//    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
//      // 拿到备选电影和最近评分电影的相似度
//      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
//      if (simScore > 0.6) {
//        // 计算备选电影的基础推荐得分
//        scores += ((candidateMovie, simScore * userRecentlyRating._2))
//        if (userRecentlyRating._2 > 3) {
//          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
//        } else {
//          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
//        }
//      }
//    }
//    // 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
//    scores.groupBy(_._1).map {
//      // groupBy之后得到的数据  Map(mid->ArrayBuffer[(mid,score)])
//      case (mid, scoreList) => {
//        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) -
//          log(decreMap.getOrDefault(mid, 1)))
//      }
//    }.toArray.sortWith(_._2 > _._2)
//  }
//
//  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
//    // 定义到StreamRecs表的连接
//    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
//
//    // 如果表中已有uid对应的数据，则删除
//    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
//
//    // 将streamRecs数据存入表中
//    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
//      "recs" -> streamRecs.map(
//        x => MongoDBObject("mid" -> x._1, "score" -> x._2)
//      )))
//  }
//
//
//  // 获取候选电影和已评分电影的相似度
//  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: collection.Map[Int, Map[Int, Double]]): Double = {
//    simMovies.get(mid1) match {
//      case Some(sims) => sims.get(mid2) match {
//        case Some(score) => score
//        case None => 0.0
//      }
//      case None => 0.0
//    }
//  }
//
//  def log(m: Int): Double = {
//    math.log(m) / math.log(10)
//  }
//
//}
//
//// 连接助手对象 序列化
//object ConnHelper extends Serializable {
//  lazy val jedis = new Jedis("linux") // lazy 初始化会延迟
//  lazy val mongoClient =
//    MongoClient(MongoClientURI("mongodb://linux:27017/recommender"))
//}
//
//case class MongoConfig(uri: String, db: String)
//
//// 标准推荐
//case class Recommendation(mid: Int, score: Double)
//
////电影的相似度
//case class MovieRecs(mid: Int, recs: Seq[Recommendation])
//
//// 用户的推荐
//case class UserRecs(uid: Int, recs: Seq[Recommendation])

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  //入口方法
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个 SparkConf 配置
    val sparkConf = new
        SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    implicit val mongConfig = MongConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._
    // 广播电影相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]
    val simMoviesMatrix = spark
      .read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { recs =>
        (recs.mid, recs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)
    //创建到 Kafka 的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "linux:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream =
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))
    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream = kafkaStream.map { case msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    // 核心实时推荐算法
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (uid, mid, score, timestamp) =>
        println(">>>>>>>>>>>>>>>>")
        //获取当前最近的 M 次电影评分
        val userRecentlyRatings =
          getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
        //获取电影 P 最相似的 K 个电影
        val simMovies =
          getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)
        //计算待选电影的推荐优先级
        val streamRecs =
          computeMovieScores(simMoviesMatrixBroadCast.value, userRecentlyRatings, simMovies)
        //将数据保存到 MongoDB
        saveRecsToMongoDB(uid, streamRecs)
      }.count()
    }
    //启动 Streaming 程序
    ssc.start()
    println(">>>>>>>>>>>>>>>>>streaming started!")
    ssc.awaitTermination()

  }

  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从用户的队列中取出 num 个评分
    jedis.lrange("uid:" + uid.toString, 0, num).map { item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int,
                      simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongConfig: MongConfig): Array[Int] = {
    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经观看过得电影
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject
      ("uid" -> uid)).toArray.map { item =>
        item.get("mid").toString.toInt
      }
    //过滤掉已经评分过得电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 >
      _._2).take(num).map(x => x._1)
  }

  def computeMovieScores(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]):
  Array[(Int, Double)] = {
    //用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //用于保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    //用于保存每一个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()
    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      if (simScore > 0.6) {
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        } else {
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }
    score.groupBy(_._1).map { case (mid, sims) =>
      (mid, sims.map(_._2).sum / sims.length + log(increMap.getOrDefault(mid, 1)) -
        log(decreMap.getOrDefault(mid, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  def getMoviesSimScore(
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                         userRatingMovie: Int, topSimMovie: Int): Double = {
    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  //取 10 的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(10)
  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongConfig:
  MongConfig): Unit = {
    //到 StreamRecs 的连接
    val streaRecsCollection =
      ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streaRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" ->
      streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }
}

// 连接助手对象
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("linux")
  lazy val mongoClient =
    MongoClient(MongoClientURI("mongodb://linux:27017/recommender"))
}

case class MongConfig(uri: String, db: String)

// 标准推荐
case class Recommendation(mid: Int, score: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])