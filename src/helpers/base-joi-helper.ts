import Joi from "joi";

export function getJoiValidationErrors(err: Joi.ValidationError): string | null {
  if (err?.details?.length) {
    const details: Joi.ValidationErrorItem[] = JSON.parse(JSON.stringify(err.details));
    const joiData = details.map((x) => x.message.replace(new RegExp('"', "g"), ""));
    return joiData.join("; ");
  }
  return "";
}
