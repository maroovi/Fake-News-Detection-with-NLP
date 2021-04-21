package testcases
import com.project.csye7200.Data._
import com.project.csye7200.TFIDF._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
class TIFIDFSpec extends AnyFlatSpec with Matchers {

  behavior of "Methods in TFIDF"

  val textidf = tf_idf(create_dataframe(),"text")

  val titleidf = tf_idf(create_dataframe(),"title")

  val titleidf_advanced = tf_idf(textidf,"title")

  it should "The tf_idf should produce a dataframe with two columns below for text" in {
    assert(textidf.columns.contains("text_tfidf"))
    assert(textidf.columns.contains("text_ct_vectorized"))
  }

  it should "The tf_idf should produce a dataframe with two columns below for title"  in {
    assert(titleidf.columns.contains("title_tfidf"))
    assert(titleidf.columns.contains("title_ct_vectorized"))
  }

  it should "when the textidf output containing the text idf columns is passed it should produce a dataframe with below columns in the output dataframe" in {
    assert(titleidf_advanced.columns.contains("text_tfidf"))
    assert(titleidf_advanced.columns.contains("text_ct_vectorized"))
    assert(titleidf_advanced.columns.contains("title_tfidf"))
    assert(titleidf_advanced.columns.contains("title_ct_vectorized"))
  }


  it should "the pipeline stage would produce a tuple of indexer and assembler column" in {
    val (indexer,assembler) = pipeline_stages(3)
    assert(indexer != null)
    assert(assembler != null)
  }

}
