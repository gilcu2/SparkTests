/**
  * Created by gilcu2 on 12/24/16.
  */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

// To type lines
// nc -lk 9999

object WordCountStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("/tmp/streaming")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordCounts = countWords(lines, Seconds(60), Seconds(10))

    wordCounts.print()


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

  def countWords(lines: DStream[String]): DStream[(String, Int)] = countWords(lines, Seconds(10), Seconds(5))

  def countWords(lines: DStream[String], windowsDuration: Duration, slideDuration: Duration)
  : DStream[(String, Int)] = {
    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowsDuration, slideDuration, 2)
    wordCounts
  }

}
