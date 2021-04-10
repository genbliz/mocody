export class FuseGenericError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class FuseErrorUtils {
  mocody_helper_validateRequiredNumber(keyValueValidates: { [key: string]: number }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(!isNaN(Number(value)) && typeof value === "number")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new FuseGenericError(`${errors.join("; ")}.`);
    }
  }

  mocody_helper_createFriendlyError(message: string, statusCode?: number) {
    return new FuseGenericError(message);
  }

  mocody_helper_validateRequiredString(keyValueValidates: { [key: string]: string }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(value && typeof value === "string")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new FuseGenericError(`${errors.join("; ")}.`);
    }
  }
}

export const FuseErrorUtilsService = new FuseErrorUtils();
