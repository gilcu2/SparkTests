/**
  * Created by gilcu2 on 12/24/16.
  */

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object RetweetNet {

  case class RetweetFromTo(retweeter: String, retweeted: String)

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConfiguration = new SparkConf()
      .setAppName("RetweetNet")
      .setMaster("local[*]")

    // Let's create the Spark Context using the configuration we just created
    val sparkContext = new SparkContext(sparkConfiguration)

    // Now let's wrap the context in a streaming one, passing along the window size
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    ssc.checkpoint("/tmp/streaming")

    val status = TwitterUtils.createStream(ssc, None, filters)

    val retweets = status.filter(_.isRetweet)

    val retweetFromTo = retweets.map(x => {
      val retwetter = x.getUser.getScreenName
      val retweeted = x.getRetweetedStatus.getUser.getScreenName
      (RetweetFromTo(retwetter, retweeted), 1)
    })

    val retweetFromToCount: DStream[(RetweetFromTo, Int)] = retweetFromTo.reduceByKeyAndWindow(_ + _, _ - _, Minutes(60), Minutes(1), 2)

    //    val sorted=retweetFromToCount.transform(rdd=>rdd.takeOrdered(10)(Ordering[Int].reverse.on { x => x._2 }))
    val sorted = retweetFromToCount.transform(rdd =>
      rdd.groupBy(_._1.retweeted).sortBy(_._2.size, false)
        .map(x => (x._1, x._2.size, x._2.toSeq.sortBy(_._2).reverse.take(5).map(y => (y._1.retweeter, y._2)))))

    sorted.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate


  }

}
