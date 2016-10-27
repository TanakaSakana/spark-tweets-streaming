import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object collect {
  def main(args: Array[String]) {

    val filters = Array("data science")
    //val filers = "ThisIsSparkStreamingFilter_100K_per_Second"

    val delimeter = "|"

    System.setProperty("twitter4j.oauth.consumerKey", "e7KiYQz1koMZOuxNtyxu9pjyK")
    System.setProperty("twitter4j.oauth.consumerSecret", "6bHUHyQwPdxQlIOiKSVyFHNAEI2cel6qibaat3wQk2RV0ls0FO")
    System.setProperty("twitter4j.oauth.accessToken", "969272604-iw8OzM90fFCDHoHGQrBQuMXXd1q2wISXtZKj5THz")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "mBkDJe2aq1HWBX1LXRK6Vs0Mz8HvgrOGhccbFItUgUISq")
    System.setProperty("twitter4j.http.useSSL", "true")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("TwitterApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val tweetStream = TwitterUtils.createStream(ssc, None, filters)

    val tweetRecords = tweetStream.map(status => {
      def getValStr(x: Any): String = {
        if (x != null && !x.toString.isEmpty()) x.toString + "|" else "|"
      }
      var tweetRecord = getValStr(status.getText())
      tweetRecord
    })

    tweetRecords.print
    ssc.start()
    ssc.awaitTermination()
  }
}