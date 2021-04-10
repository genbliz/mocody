export class MocodyGenericError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class MocodyErrorUtils {
  mocody_helper_validateRequiredNumber(keyValueValidates: { [key: string]: number }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(!isNaN(Number(value)) && typeof value === "number")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new MocodyGenericError(`${errors.join("; ")}.`);
    }
  }

  mocody_helper_createFriendlyError(message: string, statusCode?: number) {
    return new MocodyGenericError(message);
  }

  mocody_helper_validateRequiredString(keyValueValidates: { [key: string]: string }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(value && typeof value === "string")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new MocodyGenericError(`${errors.join("; ")}.`);
    }
  }
}

export const MocodyErrorUtilsService = new MocodyErrorUtils();
