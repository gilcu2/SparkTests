/**
  * Created by gilcu2 on 12/23/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by toddmcgrath on 6/15/16.
  */
object SimpleSpark {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("/tmp/spark/")

    println("-------------Attach debugger now!--------------")
    Thread.sleep(8000)

    val logFile = "/opt/apache-spark//README.md" // Should be some file on your system
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}