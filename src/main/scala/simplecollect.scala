package com.spark.twitter

import java.io._
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object simplecollect {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("TwitterApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val accum = sc.accumulator(0)
    val cb = new ConfigurationBuilder
    val pa = new Path("sad")
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("SN0ZW4jnN6dyz2ouQUbbO0J7n")
      .setOAuthConsumerSecret("SUkHN7gMK2Yqaksoem0bwdDs02EH881pJvV5BLAbqjTHSyUjIi")
      .setOAuthAccessToken("3459651314-AVGZON8ruVIdM0hOu9a1S2Nje9uquEzmsmiVQY0")
      .setOAuthAccessTokenSecret("bYcuKxOPJgh8FMxNaF894K8zgbGQNtZsiuJvaD4Nqiwjy")
    val auth = new OAuthAuthorization(cb.build())

    val dateFormat = new SimpleDateFormat("hh:mm:ss")

    val tweets = TwitterUtils.createStream(ssc, Some(auth), Array("SEGA", "NAMCO", "BANDAI"))
    val status = tweets.map(_.getText)

    tweets.filter(_.getLang == "ja").
      filter(_.getUser.getFollowersCount>=1000).
      foreachRDD(x => {
        x.collect().foreach(delta => {
        accum+=1
        printf("\nTime: %s", dateFormat.format(delta.getCreatedAt))
        printf("\nUser: %s", delta.getUser.getName)
        printf("\nText: %S", delta.getText.replace("\n"," "))
        println("\nAccumulator: %d".format(accum.value))
        println()

        printToFile(delta)((x, fw) => {
          fw.write("\nTime: %s".format(dateFormat.format(x.getCreatedAt)))
          fw.write("\nUser: %s".format(x.getUser.getName))
          fw.write("\nText: %S\n".format(x.getText))
        })
      })
    })
    ssc.start
    ssc.awaitTermination
  }

  def printToFile(stat: Status)(op: (Status, FileWriter) => Unit) {
    val fw = new java.io.FileWriter("output.txt", true)
    try {
      op(stat, fw)
    } finally {
      fw.close()
    }
  }
}
