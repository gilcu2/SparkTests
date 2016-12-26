import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite
import WordCountStreaming.countWords

/**
  * Created by gilcu2 on 12/26/16.
  */
class TestStreaming extends FunSuite with StreamingSuiteBase {

  test("words in windows") {
    val input = List(List("es una prueba"), List("otra prueba"), List("otra más"))
    val expected = List(
      List(("es", 1), ("una", 1), ("prueba", 1)),
      List(("es", 1), ("una", 1), ("prueba", 2), ("otra", 1)),
      List(("mas", 1), ("prueba", 1), ("otra", 2))
    )
    testOperation(input, countWords _, expected, ordered = false)
  }

}
