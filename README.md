# 电影推荐系统

> 项目代码包括两个大模块 Recommender和businessServer
>
> Recommender为推荐模块的代码；businessServer为业务系统的代码

+ 由于我学习电影推荐系统的目的在于学习大数据和推荐算法，因此我主要介绍推荐算法和大数据的相关知识
+ 实现的推荐大方向上包括**离线推荐**和**实时推荐**
  + 离线推荐又包括基于统计的推荐和个性化推荐
    + 基于统计的推荐：sql
    + 个性化推荐：矩阵分解，优化方法是ALS算法
  + 实时推荐又包括基于协同过滤的推荐和基于内容的推荐
    + 基于协同过滤的推荐：具体说是item-CF，结合多方面的考虑设计了一个推荐算法
    + 基于内容的推荐：TF-IDF算法
+ 大数据的相关知识：
  + scala
  + spark core
  + spark sql
  + spark streaming
  + zookeeper
  + kafka
  + flume
  + redis
  + MongoDB
  + elasticsearch
+ 对于实时推荐的大致流程：
  + 首先用户在网站中给电影评分，后台就会收集到用户对该电影的评分，然后通过flume将数据发送到kafka中，然后kafka会有一个日志处理的过程(java实现)，然后将处理好的数据再输入到另外一个topic中，然后streaming程序就可以读到该topic中的数据进行实时推荐
