package testcases
import com.project.csye7200.Data._
import com.project.csye7200.TFIDF._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
class TIFIDFSpec extends AnyFlatSpec with Matchers {

  behavior of "Methods in TFIDF"
  val df = create_dataframe()
  val preprocessed = preprocess_data(df,"text")
  val textidf = tf_idf(preprocessed,"text")

  val preprocessed_title = preprocess_data(df,"title")
  val titleidf = tf_idf(preprocessed_title,"title")

  val preprocessed_title_text = preprocess_data(textidf,"title")
  val titleidf_advanced = tf_idf(preprocessed_title_text,"title")

  it should "contain text count vectorized and tfidf columns" in {
    assert(textidf.columns.contains("text_tfidf"))
    assert(textidf.columns.contains("text_ct_vectorized"))
  }

  it should "contain title count vectorized and tfidf columns"  in {
    assert(titleidf.columns.contains("title_tfidf"))
    assert(titleidf.columns.contains("title_ct_vectorized"))
  }

  it should "contain text and title count vectorized and tfidf columns" in {
    assert(titleidf_advanced.columns.contains("text_tfidf"))
    assert(titleidf_advanced.columns.contains("text_ct_vectorized"))
    assert(titleidf_advanced.columns.contains("title_tfidf"))
    assert(titleidf_advanced.columns.contains("title_ct_vectorized"))
  }


  it should "produce a string indexer and vector assembler" in {
    val (indexer,assembler) = pipeline_stages(3)
    assert(indexer != null)
    assert(assembler != null)
  }

}
