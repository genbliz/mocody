import Joi from "joi";

export function getJoiValidationErrors(
  err: Joi.ValidationError
): string | null {
  if (err?.details?.length) {
    const details: Joi.ValidationErrorItem[] = JSON.parse(
      JSON.stringify(err.details)
    );
    const joiData = details.map((x) =>
      x.message.replace(new RegExp('"', "g"), "")
    );
    return joiData.join("; ");
  }
  return "";
}

export function defaultISODateNowFunc() {
  return new Date().toISOString();
}

export function dateISOValidation({
  isRequired,
  defaultVal,
}: { isRequired?: boolean; defaultVal?: () => string | string } = {}) {
  if (isRequired === true) {
    if (defaultVal) {
      return Joi.string()
        .isoDate()
        .required()
        .strict(false)
        .default(defaultVal);
    }
    return Joi.string().isoDate().required().strict(false);
  }
  if (defaultVal) {
    return Joi.string().isoDate().strict(false).allow(null).default(defaultVal);
  }
  return Joi.string()
    .isoDate()
    .strict(false)
    .empty("")
    .allow(null)
    .default(null);
}
