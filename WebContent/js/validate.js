function contractValidateForm(form) {

	var descriptionField = form.descriptionContract;
	var refIDField = form.refIDContract;

	if (isNotEmpty(descriptionField) && isNotEmpty(refIDField)) {
		return true;
	}
	return false;
}

function customerValidateForm(form) {

	var nameField = form.nameCustomer;
	var locationField = form.locationCustomer;
	var zipField = form.zipCustomer;

	if (isNotEmpty(nameField) && isNotEmpty(locationField)
			&& isNotEmpty(zipField)) {
		return true;
	}
	return false;
}

function isNotEmpty(field) {

	var fieldData = field.value;

	if (fieldData.length == 0 || fieldData == "") {
		alert("Eintrag darf nicht leer sein");
		field.className = "FieldError";
		return false;
	}
	return true;
}