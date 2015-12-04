package com.linqia.spark.enhance

import com.linqia.rules.RulebookService
import com.linqia.rules.enhancers.{HashResolver, UserAgentParser, BotChecker}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val _sparkContext: SparkContext = new SparkContext(
    new SparkConf().
        setAppName("enhance").
        setMaster("local").
        set("spark.executor.memory", "180M")
  )

  def main(args: Array[String]): Unit = {
    val input = "hdfs://localhost:9000/user/hegu/2015-11-30.json"
    val api = "http://api-internal.linqia.com"

    // variables in this trait should only be the dependencies of enhancers
    trait Dependencies {
      protected[this] val botChecker = new BotChecker(new RulebookService(api))

      protected[this] val userAgentParser = new UserAgentParser()

      protected[this] val hashResolver = new HashResolver(api.substring(7))

      @transient val sparkContext: SparkContext = _sparkContext
    }

    val enhancer = new BadgeEnhancer(input, "") with Operations with Dependencies

    enhancer.run()
  }
}
