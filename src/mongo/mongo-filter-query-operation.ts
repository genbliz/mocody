import { LoggingService } from "./../helpers/logging-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition } from "../type";
import { QueryValidatorCheck } from "../helpers/query-validator";
import { MocodyErrorUtilsService } from "../helpers/errors";
// https://docs.mongodb.com/drivers/node/fundamentals/crud/

interface ISelectedQueryConditionsKeys {
  $lt?: any;
  $gt?: any;
  $lte?: any;
  $gte?: any;
  $eq?: any;
  $ne?: any;
  $exists?: boolean;
  $in?: any[];
  $nin?: any[];
  $regex?: RegExp;
  $elemMatch?: { $in: any[] };
}

type FieldPartial<T> = { [P in keyof T]-?: string };

const KEY_CONDITION_MAP: FieldPartial<IMocodyKeyConditionParams> = {
  $eq: "$eq",
  $lt: "$lt",
  $lte: "$lte",
  $gt: "$gt",
  $gte: "$gte",
  $between: "",
  $beginsWith: "",
};

const QUERY_CONDITION_MAP_PART: FieldPartial<Omit<IMocodyQueryConditionParams, keyof IMocodyKeyConditionParams>> = {
  $ne: "$ne",
  $exists: "",
  $in: "",
  $nin: "",
  $contains: "",
  $notContains: "",
  $elemMatch: "",
  $nestedMatch: "",
  $nestedArrayMatch: "",
};

const QUERY_CONDITION_MAP_FULL = { ...KEY_CONDITION_MAP, ...QUERY_CONDITION_MAP_PART };

type FieldPartialQuery<T> = { [P in keyof T]-?: T[P] };
type IQueryConditions = {
  [fieldName: string]: FieldPartialQuery<ISelectedQueryConditionsKeys>;
};

function getQueryConditionExpression(key: string): string | null {
  if (key && Object.keys(QUERY_CONDITION_MAP_FULL).includes(key)) {
    const conditionExpr = QUERY_CONDITION_MAP_FULL[key];
    if (conditionExpr) {
      return conditionExpr;
    }
  }
  return null;
}

export class MongoFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const result = {
      [fieldName]: { $exists: true },
    } as IQueryConditions;
    return result;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const result = {
      [fieldName]: { $exists: false },
    } as IQueryConditions;
    return result;
  }

  private operation__helperFilterBasic({
    fieldName,
    val,
    conditionExpr,
  }: {
    fieldName: string;
    conditionExpr?: string;
    val: string | number;
  }): IQueryConditions {
    if (conditionExpr) {
      return {
        [fieldName]: { [conditionExpr]: val },
      } as any;
    }
    const result = {
      [fieldName]: { $eq: val },
    } as IQueryConditions;
    return result;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const result = {
      [fieldName]: { $in: attrValues },
    } as IQueryConditions;
    return result;
  }

  private operation__filterElementMatch({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: { $in: any[] };
  }): IQueryConditions {
    const result = {
      [fieldName]: {
        $elemMatch: attrValues,
      },
    } as IQueryConditions;
    return result;
  }

  private operation__filterNotIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const result = {
      [fieldName]: { $nin: attrValues },
    } as IQueryConditions;
    return result;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    const result = {
      [fieldName]: { $regex: new RegExp(`${term}`, "ig") },
    } as IQueryConditions;
    return result;
  }

  private operation__filterNotContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    const result = {
      [fieldName]: { $not: { $regex: new RegExp(`${term}`, "ig") } } as any,
    } as IQueryConditions;
    return result;
  }

  private operation__filterBetween({ fieldName, from, to }: { fieldName: string; from: any; to: any }): IQueryConditions {
    const result = {
      [fieldName]: { $gte: from, $lte: to },
    } as IQueryConditions;
    return result;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const result = {
      [fieldName]: { $regex: new RegExp(`^${term}`, "i") },
    } as IQueryConditions;
    return result;
  }

  private operation__filterNestedMatchObject({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: Record<string, Record<string, any> | string | number>; // { product: {$eq: "xyz"} }
  }): IQueryConditions[] {
    const results: IQueryConditions[] = [];
    Object.entries(attrValues).forEach(([subFieldName, queryval]) => {
      //
      let queryValue01: Record<string, any>;

      if (queryval && typeof queryval === "object") {
        queryValue01 = { ...queryval };
      } else {
        queryValue01 = { $eq: queryval };
      }

      Object.entries(queryValue01).forEach(([condKey, conditionValue]) => {
        //
        const conditionKey = condKey as keyof IMocodyKeyConditionParams;
        //
        if (!Object.keys(KEY_CONDITION_MAP).includes(conditionKey)) {
          throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
            `Invalid query key: ${conditionKey} @ NestedMatchObject`,
          );
        }
        const conditionExpr = KEY_CONDITION_MAP[conditionKey];

        if (conditionExpr) {
          const result = {
            [`${fieldName}.${subFieldName}`]: { [conditionExpr]: conditionValue },
          } as IQueryConditions;
          results.push(result);
        } else {
          if (conditionKey === "$between") {
            QueryValidatorCheck.between(conditionValue);

            const [fromVal, toVal] = conditionValue;
            const result = {
              [`${fieldName}.${subFieldName}`]: { $gte: fromVal, $lte: toVal },
            } as IQueryConditions;
            results.push(result);
            //
          } else if (conditionKey === "$beginsWith") {
            QueryValidatorCheck.beginWith(conditionValue);

            const result = {
              [`${fieldName}.${subFieldName}`]: { $regex: new RegExp(`^${conditionValue}`, "i") },
              // [`${fieldName}.${subFieldName}`]: { $regex: `^${conditionValue}`, $options: "i" } as any,
            } as IQueryConditions;
            results.push(result);
          } else {
            throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(`Query key: ${conditionKey} not currently supported`);
          }
        }
      });
    });
    return results;
  }

  private operation__translateAdvancedQueryOperation({
    fieldName,
    queryObject,
  }: {
    fieldName: string;
    queryObject: Record<string, any>;
  }) {
    const queryConditionsAdvanced: IQueryConditions[] = [];
    Object.entries(queryObject).forEach(([condKey, conditionValue]) => {
      const conditionKey = condKey as keyof IMocodyQueryConditionParams;
      if (conditionValue !== undefined) {
        if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);

          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from: conditionValue[0],
            to: conditionValue[1],
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$beginsWith") {
          QueryValidatorCheck.beginWith(conditionValue);

          const _queryConditions = this.operation__filterBeginsWith({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$contains") {
          QueryValidatorCheck.contains(conditionValue);

          const _queryConditions = this.operation__filterContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$notContains") {
          QueryValidatorCheck.notContains(conditionValue);

          const _queryConditions = this.operation__filterNotContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$in") {
          QueryValidatorCheck.in_query(conditionValue);
          const _queryConditions = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$nin") {
          QueryValidatorCheck.notIn(conditionValue);
          const _queryConditions = this.operation__filterNotIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$elemMatch") {
          QueryValidatorCheck.elemMatch(conditionValue);
          const _queryConditions = this.operation__filterElementMatch({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditionsAdvanced.push(_queryConditions);
        } else if (conditionKey === "$nestedMatch") {
          QueryValidatorCheck.nestedMatch(conditionValue);
          const nestedMatchConditions = this.operation__filterNestedMatchObject({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (nestedMatchConditions?.length) {
            queryConditionsAdvanced.push(...nestedMatchConditions);
          }
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);
          if (String(conditionValue) === "true") {
            const _queryConditions = this.operation__filterFieldExist({
              fieldName: fieldName,
            });
            queryConditionsAdvanced.push(_queryConditions);
          } else if (String(conditionValue) === "false") {
            const _queryConditions = this.operation__filterFieldNotExist({
              fieldName: fieldName,
            });
            queryConditionsAdvanced.push(_queryConditions);
          }
        } else {
          const conditionExpr = getQueryConditionExpression(conditionKey);
          if (conditionExpr) {
            const _queryConditions = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            queryConditionsAdvanced.push(_queryConditions);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });
    LoggingService.logAsString({ queryConditionsAdvanced });
    return queryConditionsAdvanced;
  }

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const _queryConditions = this.operation__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      // conditionExpr: "$eq",
    });
    return _queryConditions;
  }

  processQueryFilter({ queryDefs }: { queryDefs: IMocodyQueryDefinition<any>["query"] }) {
    const queryMainConditions: IQueryConditions[] = [];
    const queryAndConditions: IQueryConditions[] = [];
    const queryOrConditions: IQueryConditions[] = [];

    Object.entries(queryDefs).forEach(([conditionKey, conditionValue]) => {
      if (conditionKey === "$or") {
        const orArray = conditionValue as IQueryConditions[];

        QueryValidatorCheck.or_query(orArray);

        orArray.forEach((orQuery) => {
          Object.entries(orQuery).forEach(([fieldName, orQueryObjectOrValue]) => {
            if (orQueryObjectOrValue !== undefined) {
              if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                const orQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                queryOrConditions.push(...orQueryCond01);
              } else {
                const orQueryCondition02 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                queryOrConditions.push(orQueryCondition02);
              }
            }
          });
        });
      } else if (conditionKey === "$and") {
        const andArray = conditionValue as IQueryConditions[];

        QueryValidatorCheck.and_query(conditionValue);

        andArray.forEach((andQuery) => {
          Object.entries(andQuery).forEach(([fieldName, andQueryObjectOrValue]) => {
            //
            if (andQueryObjectOrValue !== undefined) {
              if (andQueryObjectOrValue && typeof andQueryObjectOrValue === "object") {
                const _andQueryCond = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                queryAndConditions.push(..._andQueryCond);
              } else {
                const _andQueryConditions = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                queryAndConditions.push(_andQueryConditions);
              }
            }
          });
        });
      } else {
        if (conditionKey && conditionValue !== undefined) {
          if (conditionValue && typeof conditionValue === "object") {
            const _queryCond = this.operation__translateAdvancedQueryOperation({
              fieldName: conditionKey,
              queryObject: conditionValue,
            });
            queryMainConditions.push(..._queryCond);
          } else {
            const _queryConditions = this.operation_translateBasicQueryOperation({
              fieldName: conditionKey,
              queryObject: conditionValue,
            });
            queryMainConditions.push(_queryConditions);
          }
        }
      }
    });

    let queryAllConditions: IQueryConditions & { $and: IQueryConditions[] } & { $or: IQueryConditions[] } = {} as any;

    if (queryMainConditions?.length) {
      queryMainConditions.forEach((item1) => {
        if (item1) {
          queryAllConditions = { ...queryAllConditions, ...item1 };
        }
      });
    }

    if (queryAndConditions?.length) {
      queryAllConditions.$and = queryAndConditions;
    }

    if (queryOrConditions?.length) {
      queryAllConditions.$or = queryOrConditions;
    }
    LoggingService.logAsString({ queryAllConditions });
    return queryAllConditions;
  }
}
