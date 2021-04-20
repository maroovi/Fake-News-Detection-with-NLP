package controllers

import javax.inject.Inject
import javax.inject.Singleton

import models.InputTypesForm
import play.api.libs.Files
import play.api.mvc._

@Singleton
class BasicController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with play.api.i18n.I18nSupport {

  def index() = Action { implicit request =>
    Ok(views.html.inputsTypesForm(InputTypesForm.form))
  }

  def inputTypesForm() = Action { implicit request =>
    Ok(views.html.inputsTypesForm(InputTypesForm.form))
  }

  def inputTypesFormPost() = Action { implicit request =>
    InputTypesForm.form.bindFromRequest.fold(
      formWithErrors => {
        // binding failure, you retrieve the form containing errors:
        BadRequest(views.html.inputsTypesForm(formWithErrors))
      },
      formData => {
        Ok(formData.toString)
      }
    )
  }
}
