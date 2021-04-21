function myFunction() {
  alert("The form was submitted");
}
/**
 * This functions loads the result in the HTML component.
 */
function loadResult(doc){
  jQuery.get( "http://localhost:9000/rnd/rxbat", function(
      response ) {
    doc.getElementById("verdict").value = "FALSE";
  }).fail(function(e) {
    alert('Wops! We was not able to call http://localhost:9000/rnd/rxba. Error: ' + e.statusText);
        });
}