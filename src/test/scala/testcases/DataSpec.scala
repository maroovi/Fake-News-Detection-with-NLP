package testcases
import com.project.csye7200.Data._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataSpec extends AnyFlatSpec with Matchers {

  behavior of "Methods in Data"

  val inputData = create_dataframe()

  it should "work for entire data" in{
    val size = inputData.count()
    println("trainData size: " + size)
    assert(size > 0)
  }

  it should "work for proceesed text data" in {
    val processed_data = preprocess_data(inputData,"text")
    assert(processed_data.columns.contains("text_words"))
    assert(processed_data.columns.contains("text_sw_removed"))
  }

  it should "work for processed title data" in {
    val processed_data = preprocess_data(inputData,"title")
    assert(processed_data.columns.contains("title_words"))
    assert(processed_data.columns.contains("title_sw_removed"))
  }
}