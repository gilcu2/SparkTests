import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by gilcu2 on 12/26/16.
  */
class TestStreaming extends FunSuite with StreamingSuiteBase {

  override def maxWaitTimeMillis: Int = 10000

  test("words in windows") {

    def countWords(lines: DStream[String]): DStream[(String, Int)] =
      WordCountStreaming.countWords(lines, Seconds(2), Seconds(1))

    val input = List(List("es una prueba"), List("otra prueba"), List("otra más"))
    val expected = List(
      List(("es", 1), ("una", 1), ("prueba", 1)),
      List(("es", 1), ("una", 1), ("prueba", 2), ("otra", 1)),
      List(("más", 1), ("prueba", 1), ("otra", 2), ("es", 0), ("una", 0))
    )
    testOperation(input, countWords _, expected, ordered = false)
  }


}
