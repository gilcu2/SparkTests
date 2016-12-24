/**
  * Created by gilcu2 on 12/24/16.
  */

import org.apache.spark._
import org.apache.spark.streaming._

// To type lines
// nc -lk 9999

object WordCountStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("/tmp/streaming")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 5), ("world", 1)))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(5), 2)

    wordCounts.print()


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
