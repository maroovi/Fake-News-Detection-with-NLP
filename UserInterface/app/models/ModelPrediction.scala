package models
import play.api.libs.json.Json
case class ModelPrediction(
                           news:String,
                           subject:String,
                           model:String
                          )
object ModelPrediction{
  implicit val modelFormat = Json.format[ModelPrediction]
}