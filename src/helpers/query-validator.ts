import { MocodyErrorUtilsService } from "./errors";

class QueryValidatorCheckBase {
  private queryErrorThrowChecks({ conditionValue, queryType }: { conditionValue: any; queryType: string }) {
    throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
      `Value: ${JSON.stringify(conditionValue)}, is invalid for ${queryType} query`,
    );
  }

  beginWith(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$beginsWith" });
    }
  }

  between(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length === 2)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$between" });
    }
  }

  contains(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$contains" });
    }
  }

  notContains(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "string")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$notContains" });
    }
  }

  in_query(conditionValue: unknown) {
    if (conditionValue && Array.isArray(conditionValue) && conditionValue.length) {
      const firstValue = conditionValue[0];
      if (typeof firstValue !== "string" || typeof firstValue !== "number") {
        this.queryErrorThrowChecks({ conditionValue, queryType: "$in" });
      }
    } else {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$in" });
    }
  }

  notIn(conditionValue: unknown) {
    if (conditionValue && Array.isArray(conditionValue) && conditionValue.length) {
      const firstValue = conditionValue[0];
      if (typeof firstValue !== "string" || typeof firstValue !== "number") {
        this.queryErrorThrowChecks({ conditionValue, queryType: "$nin" });
      }
    } else {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$nin" });
    }
  }

  elemMatch(conditionValue: { $in: any[] }) {
    if (!(conditionValue && typeof conditionValue === "object")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$elemMatch" });
    }
    if (!(conditionValue?.$in?.length && Array.isArray(conditionValue.$in))) {
      throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
        "$elemMatch must have a valid $in query and must be an array of non-zero length",
      );
    }

    for (const item of conditionValue.$in) {
      if (typeof item !== "number" && typeof item !== "string" && typeof item !== "boolean") {
        throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
          "$in in $elemMatch MUST have values of string or number or boolean",
        );
      }
    }
  }

  nestedMatch(conditionValue: Record<string, any>) {
    if (!(conditionValue && typeof conditionValue === "object")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$nestedMatch" });
    }
    if (!Object.keys(conditionValue).length) {
      throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
        "$nestedMatch must have a valid query definitions",
      );
    }
  }

  not_query(conditionValue: unknown) {
    if (!(conditionValue && typeof conditionValue === "object")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$not" });
    }
  }

  exists(conditionValue: unknown) {
    if (!(String(conditionValue) === "true" || String(conditionValue) === "false")) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$exists" });
    }
  }

  or_query(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$or" });
    }
  }

  and_query(conditionValue: unknown) {
    if (!(conditionValue && Array.isArray(conditionValue) && conditionValue.length)) {
      this.queryErrorThrowChecks({ conditionValue, queryType: "$and" });
    }
  }

  throwQueryNotFound(queryType: any) {
    throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
      `Query type: ${JSON.stringify(queryType)}, not supported`,
    );
  }
}

export const QueryValidatorCheck = new QueryValidatorCheckBase();
