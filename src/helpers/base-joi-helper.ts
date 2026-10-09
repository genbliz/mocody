import type Joi from "joi";

const fieldPlaceholderWithLabel = process.env.MOCODY_SCHEMA_VALIDATE_FIELD_PLACEHOLDER_WITH_LABEL || "";
const fieldPlaceholder = process.env.MOCODY_SCHEMA_VALIDATE_FIELD_PLACEHOLDER || "";

export function getJoiValidationErrors(err: Joi.ValidationError): string | null {
  if (err?.details?.length) {
    if (fieldPlaceholderWithLabel && fieldPlaceholder) {
      return getJoiValidationErrorAdvanced(err);
    }
    const details: Joi.ValidationErrorItem[] = JSON.parse(JSON.stringify(err.details));
    const joiData = details.map((x) => x.message.replace(new RegExp('"', "g"), ""));
    return joiData.join("; ");
  }
  return null;
}

function getJoiValidationErrorAdvanced(err: Joi.ValidationError): string | null {
  if (err?.details?.length) {
    const details: Joi.ValidationErrorItem[] = JSON.parse(JSON.stringify(err.details));

    const joiData = details.map((issue) => {
      const fieldName = issue.path.join(".");

      const issueMessage = __get_val_friendly_validation_message({
        fieldName,
        message: issue.message.replaceAll('"', ""),
      });
      return issueMessage;
    });
    return joiData.join("; ");
  }
  return null;
}

function __get_val_friendly_validation_message({ fieldName, message }: { fieldName: string; message: string }) {
  if (message?.includes(fieldPlaceholderWithLabel)) {
    return message.replaceAll(fieldPlaceholderWithLabel, "");
  }

  if (message?.includes(fieldPlaceholder)) {
    return message.replaceAll(fieldPlaceholder, fieldName);
  }

  return `${fieldName}: ${message}`;
}
