/**
 * This functions loads the result in the HTML component.
 */
function loadResult(doc){
  jQuery.get("/InputTypesFormPost", function(response) {
    alert(response.prediction);
    doc.getElementById("verdict").value = "FALSE";
  }).fail(function(e) {
    alert('Wops! We was not able to call. Error: ' + e.statusText);
        });
}