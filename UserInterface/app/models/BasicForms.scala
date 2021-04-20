package models

import java.io.File

import play.api.data.Form
import play.api.data.Forms._

case class InputTypesForm(news: String, model: String)

object InputTypesForm {

  object Preferences extends Enumeration {
    val Model1, Model2 = Value
  }

  val form: Form[InputTypesForm] = Form(
    mapping(
      "news" -> nonEmptyText,
      "model" -> nonEmptyText
    )(InputTypesForm.apply)(InputTypesForm.unapply)
  )
}
