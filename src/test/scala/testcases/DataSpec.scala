package testcases
import
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Try, _}

class DataSpec extends AnyFlatSpec with Matchers {
  behavior of "Methods in Data"

  it should """match Success(1234) for parse "12" to int and parse "34" to int,with (a:Int,b:Int) => a.toString()+b.toString()""" in {
    val a1 = "12"
    val a2 = "34"
    val t1 = Try(a1.toInt)
    val t2 = Try(a2.toInt)

    val test = Function.map2(t1, t2)((a: Int, b: Int) => a.toString + b.toString)

    test should matchPattern {
      case Success("1234") =>
    }
  }

}