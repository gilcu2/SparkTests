/**
  * Created by gilcu2 on 12/24/16.
  */

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
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

    val retweetFromToCount: DStream[(RetweetFromTo, Int)] = retweetFromTo
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(60), Minutes(1), 2).cache()

    //    val sorted=retweetFromToCount.transform(rdd=>rdd.takeOrdered(10)(Ordering[Int].reverse.on { x => x._2 }))
    val retweeteds = retweetFromToCount.transform(rdd =>
      rdd.groupBy(_._1.retweeted).sortBy(_._2.size, false)
        .map(x => (x._1, x._2.size, x._2.toSeq.sortBy(_._2).reverse.take(5).map(y => (y._1.retweeter, y._2)))))

    retweeteds.print()

    val retweeters: DStream[(VertexId, List[(String, Int)])] = retweetFromToCount.transform(rdd => {
      val mapIdent2Index = (rdd.map(_._1.retweeter) ++ rdd.map(_._1.retweeted)).distinct.zipWithIndex
      val nodes = mapIdent2Index.map(x => (x._2, x._1))

      // replace nodes Id by node index needed for edges
      val edges0 = rdd.map(x => (x._1.retweeter, (x._1.retweeted, x._2))).join(mapIdent2Index).map(x => {
        val retweetedId = x._2._1._1
        val nRetweets = x._2._1._2
        val retweeterIndex = x._2._2
        (retweetedId, (retweeterIndex, nRetweets))
      })

      val edges = edges0.join(mapIdent2Index).map(x => {
        val retweeterIndex = x._2._1._1
        val nRetweets = x._2._1._2
        val retweetedIndex = x._2._2
        Edge(retweeterIndex, retweetedIndex, nRetweets)
      })

      val graph = Graph(nodes, edges)
      graph.aggregateMessages[List[(String, Int)]](
        triplet => {
          triplet.sendToSrc(List((triplet.dstAttr, triplet.attr)))
        } //map
        ,
        (a, b) => a ++ b
      ).sortBy(-_._2.size)

    })

    retweeters.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate


  }

}

//(String, ((Long, Int), Long))