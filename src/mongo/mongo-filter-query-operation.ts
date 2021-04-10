import { LoggingService } from "./../helpers/logging-service";
import type { IFuseKeyConditionParams, IFuseQueryConditionParams, IFuseQueryDefinition } from "../type/types";
import { QueryValidatorCheck } from "../helpers/query-validator";
import { FuseErrorUtilsService } from "../helpers/errors";
// https://docs.mongodb.com/drivers/node/fundamentals/crud/

interface ISelectedQueryConditionsKeys {
  $lt?: any;
  $gt?: any;
  $lte?: any;
  $gte?: any;
  $eq?: any;
  $ne?: any;
  $not?: any;
  $exists?: boolean;
  $in?: any[];
  $nin?: any[];
  $regex?: RegExp;
  $elemMatch?: { $in: any[] };
}

type FieldPartial<T> = { [P in keyof T]-?: string };

const keyConditionMap: FieldPartial<IFuseKeyConditionParams> = {
  $eq: "$eq",
  $lt: "$lt",
  $lte: "$lte",
  $gt: "$gt",
  $gte: "$gte",
  $between: "",
  $beginsWith: "",
};

const conditionMapPre: FieldPartial<Omit<IFuseQueryConditionParams, keyof IFuseKeyConditionParams>> = {
  $ne: "$ne",
  $exists: "",
  $in: "",
  $nin: "",
  $not: "",
  $contains: "",
  $notContains: "",
  $elemMatch: "",
  $nestedMatch: "",
};

const conditionMap = { ...keyConditionMap, ...conditionMapPre };

type FieldPartialQuery<T> = { [P in keyof T]-?: T[P] };
type IQueryConditions = {
  [fieldName: string]: FieldPartialQuery<ISelectedQueryConditionsKeys>;
};

function hasQueryKeyCondition(key: string) {
  return Object.keys(keyConditionMap).includes(key);
}

function getQueryConditionExpression(key: string): string | null {
  if (key && Object.keys(conditionMap).includes(key)) {
    const conditionExpr = conditionMap[key];
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

  private operation__filterNotIn({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: any[];
  }): IQueryConditions {
    const result = {
      [fieldName]: { $nin: attrValues },
    } as IQueryConditions;
    return result;
  }

  private operation__filterNot({
    fieldName,
    selectorObjValues,
  }: {
    fieldName: string;
    selectorObjValues: any;
  }): IQueryConditions | null {
    const selector: Record<keyof IFuseKeyConditionParams, any> = { ...selectorObjValues };

    const mConditions: IQueryConditions[] = [];

    Object.entries(selector).forEach(([conditionKey, conditionValue]) => {
      if (hasQueryKeyCondition(conditionKey)) {
        const _conditionKey01 = conditionKey as keyof IFuseKeyConditionParams;

        if (_conditionKey01 === "$beginsWith") {
          QueryValidatorCheck.beginWith(_conditionKey01);
          const _queryConditions = this.operation__filterBeginsWith({
            fieldName: fieldName,
            term: conditionValue,
          });
          mConditions.push(_queryConditions);
        } else if (_conditionKey01 === "$between") {
          QueryValidatorCheck.between(_conditionKey01);
          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from: conditionValue[0],
            to: conditionValue[1],
          });
          mConditions.push(_queryConditions);
        } else {
          const conditionExpr: string = keyConditionMap[conditionKey];
          if (conditionExpr) {
            const _queryConditions = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            mConditions.push(_queryConditions);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });

    if (mConditions.length) {
      let selectorValuesAll: any = {};
      for (const condition of mConditions) {
        selectorValuesAll = { ...selectorValuesAll, ...condition[fieldName] };
      }
      const result = {
        [fieldName]: { $not: selectorValuesAll },
      } as IQueryConditions;
      return result;
    }
    return null;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    const result = {
      [fieldName]: { $regex: new RegExp(`${term}`, "ig") },
    } as IQueryConditions;
    return result;
  }

  private operation__filterNotContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    const result = {
      [fieldName]: { $not: { $regex: new RegExp(`${term}`, "ig") } },
    } as IQueryConditions;
    return result;
  }

  private operation__filterBetween({
    fieldName,
    from,
    to,
  }: {
    fieldName: string;
    from: any;
    to: any;
  }): IQueryConditions {
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
      let _queryValue: Record<string, any>;

      if (queryval && typeof queryval === "object") {
        _queryValue = { ...queryval };
      } else {
        _queryValue = { $eq: queryval };
      }

      Object.entries(_queryValue).forEach(([condKey, val]) => {
        //
        const conditionKey = condKey as keyof IFuseKeyConditionParams;
        //
        if (!Object.keys(keyConditionMap).includes(conditionKey)) {
          throw FuseErrorUtilsService.mocody_helper_createFriendlyError(
            `Invalid query key: ${conditionKey} @ NestedMatchObject`,
          );
        }
        const conditionExpr = keyConditionMap[conditionKey];

        if (conditionExpr) {
          const result = {
            [`${fieldName}.${subFieldName}`]: { [conditionExpr]: val },
          } as IQueryConditions;
          results.push(result);
        } else {
          if (conditionKey === "$between") {
            if (!(Array.isArray(val) && val.length === 2)) {
              throw FuseErrorUtilsService.mocody_helper_createFriendlyError(
                "$between query must be an array of length 2",
              );
            }
            const [fromVal, toVal] = val;
            const result = {
              [`${fieldName}.${subFieldName}`]: { $gte: fromVal, $lte: toVal },
            } as IQueryConditions;
            results.push(result);
            //
          } else if (conditionKey === "$beginsWith") {
            const result = {
              [`${fieldName}.${subFieldName}`]: { $regex: new RegExp(`^${val}`, "i") },
            } as IQueryConditions;
            results.push(result);
          } else {
            throw FuseErrorUtilsService.mocody_helper_createFriendlyError(
              `Query key: ${conditionKey} not currently supported`,
            );
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
    const queryConditions: IQueryConditions[] = [];
    Object.entries(queryObject).forEach(([condKey, conditionValue]) => {
      const conditionKey = condKey as keyof IFuseQueryConditionParams;
      if (conditionValue !== undefined) {
        if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from: conditionValue[0],
            to: conditionValue[1],
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$beginsWith") {
          QueryValidatorCheck.beginWith(conditionValue);
          const _queryConditions = this.operation__filterBeginsWith({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$contains") {
          QueryValidatorCheck.contains(conditionValue);
          const _queryConditions = this.operation__filterContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$notContains") {
          QueryValidatorCheck.notContains(conditionValue);
          const _queryConditions = this.operation__filterNotContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$in") {
          QueryValidatorCheck.in_query(conditionValue);
          const _queryConditions = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$nin") {
          QueryValidatorCheck.notIn(conditionValue);
          const _queryConditions = this.operation__filterNotIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$elemMatch") {
          QueryValidatorCheck.elemMatch(conditionValue);
          const _queryConditions = this.operation__filterElementMatch({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$nestedMatch") {
          QueryValidatorCheck.nestedMatch(conditionValue);
          const nestedMatchConditions = this.operation__filterNestedMatchObject({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (nestedMatchConditions?.length) {
            for (const _queryCondition of nestedMatchConditions) {
              queryConditions.push(_queryCondition);
            }
          }
        } else if (conditionKey === "$not") {
          QueryValidatorCheck.not_query(conditionValue);
          const _queryConditions = this.operation__filterNot({
            fieldName: fieldName,
            selectorObjValues: conditionValue,
          });
          if (_queryConditions) {
            queryConditions.push(_queryConditions);
          }
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);
          if (String(conditionValue) === "true") {
            const _queryConditions = this.operation__filterFieldExist({
              fieldName: fieldName,
            });
            queryConditions.push(_queryConditions);
          } else if (String(conditionValue) === "false") {
            const _queryConditions = this.operation__filterFieldNotExist({
              fieldName: fieldName,
            });
            queryConditions.push(_queryConditions);
          }
        } else {
          const conditionExpr = getQueryConditionExpression(conditionKey);
          if (conditionExpr) {
            const _queryConditions = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            queryConditions.push(_queryConditions);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });
    return queryConditions;
  }

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const _queryConditions = this.operation__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      // conditionExpr: "$eq",
    });
    return _queryConditions;
  }

  processQueryFilter({ queryDefs }: { queryDefs: IFuseQueryDefinition<any>["query"] }) {
    let queryMainConditions: IQueryConditions[] = [];
    let queryAndConditions: IQueryConditions[] = [];
    let queryOrConditions: IQueryConditions[] = [];

    Object.keys(queryDefs).forEach((fieldName_Or_And) => {
      if (fieldName_Or_And === "$or") {
        const orKey = fieldName_Or_And;
        const orArray: IQueryConditions[] = queryDefs[orKey];
        QueryValidatorCheck.exists(orArray);
        if (orArray && Array.isArray(orArray)) {
          orArray.forEach((orQuery) => {
            Object.keys(orQuery).forEach((fieldName) => {
              //
              const orQueryObjectOrValue = orQuery[fieldName];
              //
              if (orQueryObjectOrValue !== undefined) {
                if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                  const _orQueryCond = this.operation__translateAdvancedQueryOperation({
                    fieldName,
                    queryObject: orQueryObjectOrValue,
                  });
                  queryOrConditions = [...queryOrConditions, ..._orQueryCond];
                } else {
                  const _orQueryConditions = this.operation_translateBasicQueryOperation({
                    fieldName,
                    queryObject: orQueryObjectOrValue,
                  });
                  queryOrConditions = [...queryOrConditions, _orQueryConditions];
                }
              }
            });
          });
        }
      } else if (fieldName_Or_And === "$and") {
        const andKey = fieldName_Or_And;
        const andArray: IQueryConditions[] = queryDefs[andKey];
        QueryValidatorCheck.exists(andArray);
        if (andArray && Array.isArray(andArray)) {
          andArray.forEach((andQuery) => {
            Object.keys(andQuery).forEach((fieldName) => {
              //
              const andQueryObjectOrValue = andQuery[fieldName];
              //
              if (andQueryObjectOrValue !== undefined) {
                if (andQueryObjectOrValue && typeof andQueryObjectOrValue === "object") {
                  const _andQueryCond = this.operation__translateAdvancedQueryOperation({
                    fieldName,
                    queryObject: andQueryObjectOrValue,
                  });
                  queryAndConditions = [...queryAndConditions, ..._andQueryCond];
                } else {
                  const _andQueryConditions = this.operation_translateBasicQueryOperation({
                    fieldName,
                    queryObject: andQueryObjectOrValue,
                  });
                  queryAndConditions = [...queryAndConditions, _andQueryConditions];
                }
              }
            });
          });
        }
      } else {
        if (fieldName_Or_And) {
          const fieldName2 = fieldName_Or_And;
          const queryObjectOrValue = queryDefs[fieldName2];
          if (queryObjectOrValue !== undefined) {
            if (queryObjectOrValue && typeof queryObjectOrValue === "object") {
              const _queryCond = this.operation__translateAdvancedQueryOperation({
                fieldName: fieldName2,
                queryObject: queryObjectOrValue,
              });
              queryMainConditions = [...queryMainConditions, ..._queryCond];
            } else {
              const _queryConditions = this.operation_translateBasicQueryOperation({
                fieldName: fieldName2,
                queryObject: queryObjectOrValue,
              });
              queryMainConditions = [...queryMainConditions, _queryConditions];
            }
          }
        }
      }
    });

    let queryAllConditions: IQueryConditions & { $and: IQueryConditions[] } & { $or: IQueryConditions[] } = {} as any;

    if (queryMainConditions?.length) {
      for (const item1 of queryMainConditions) {
        if (item1) {
          queryAllConditions = { ...queryAllConditions, ...item1 };
        }
      }
    }

    if (queryAndConditions?.length) {
      queryAllConditions.$and = queryAndConditions;
    }

    if (queryOrConditions?.length) {
      queryAllConditions.$or = queryOrConditions;
    }
    LoggingService.log(JSON.stringify({ queryAllConditions }, null, 2));
    return queryAllConditions;
  }
}
