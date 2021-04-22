package models

import java.io.File

import play.api.data.Form
import play.api.data.Forms._

case class InputTypesForm(news: String, subject: String, model: String)

object InputTypesForm {

  object Preferences extends Enumeration {
    val RandomForest, NaiveBayes = Value
  }

  object MorePreferences extends Enumeration {
    val US_News, World_News, Politics, Government_News, Middle_East = Value
  }

  val form: Form[InputTypesForm] = Form(
    mapping(
      "news" -> nonEmptyText,
      "subject" -> nonEmptyText,
      "model" -> nonEmptyText
    )(InputTypesForm.apply)(InputTypesForm.unapply)
  )
}
