import { QueryValidatorCheck } from "./../helpers/query-validator";
import { LoggingService } from "../helpers/logging-service";
import { UtilService } from "../helpers/util-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition } from "../type";
import { MocodyErrorUtilsService } from "../helpers/errors";
import { getDynamoRandomKeyOrHash } from "./dynamo-helper";

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

type FieldPartial<T> = { [P in keyof T]-?: string };

const KEY_CONDITION_MAP: FieldPartial<IMocodyKeyConditionParams> = {
  $eq: "=",
  $lt: "<",
  $lte: "<=",
  $gt: ">",
  $gte: ">=",
  $beginsWith: "",
  $between: "",
} as const;

const QUERY_CONDITION_MAP_PART: FieldPartial<Omit<IMocodyQueryConditionParams, keyof IMocodyKeyConditionParams>> = {
  $ne: "<>",
  $exists: "",
  $in: "",
  $nin: "",
  $not: "",
  $contains: "",
  $notContains: "",
  $elemMatch: "",
  $nestedMatch: "",
} as const;

const QUERY_CONDITION_MAP_NESTED = {
  ...KEY_CONDITION_MAP,
  $ne: "<>",
  $contains: "",
  $exists: "",
  $in: "",
  $nin: "",
} as const;

const QUERY_CONDITION_MAP_FULL = { ...KEY_CONDITION_MAP, ...QUERY_CONDITION_MAP_PART } as const;

type IDictionaryAttr = { [key: string]: any };
type IQueryConditions = {
  xExpressionAttributeValues: IDictionaryAttr;
  xExpressionAttributeNames: IDictionaryAttr;
  xFilterExpression: string;
};

function hasQueryConditionValue(key: string) {
  if (key && Object.keys(QUERY_CONDITION_MAP_FULL).includes(key) && QUERY_CONDITION_MAP_FULL[key]) {
    return true;
  }
  return false;
}

export class DynamoFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = getDynamoRandomKeyOrHash("#");
    const result = {
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `attribute_exists (${attrKeyHash})`,
    } as IQueryConditions;
    return result;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = getDynamoRandomKeyOrHash("#");
    const result = {
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `attribute_not_exists (${attrKeyHash})`,
    } as IQueryConditions;
    return result;
  }

  private operation__helperFilterBasic({
    fieldName,
    val,
    conditionExpr,
  }: {
    fieldName: string;
    conditionExpr: string;
    val: string | number;
  }): IQueryConditions {
    const keyAttr = getDynamoRandomKeyOrHash(":");
    const attrKeyHash = getDynamoRandomKeyOrHash("#");
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: val,
      },
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: [attrKeyHash, conditionExpr, keyAttr].join(" "),
    };
    return result;
  }

  //@ts-ignore
  private operation__filterIn__001({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: (string | number)[];
  }): IQueryConditions {
    const expressAttrVal: { [key: string]: { S: string } | { N: number } } = {};
    const expressAttrName: { [key: string]: string } = {};
    const valuesVariable: string[] = [];

    const attrKeyVariale = getDynamoRandomKeyOrHash("#");
    expressAttrName[attrKeyVariale] = fieldName;

    attrValues.forEach((item, i) => {
      const keyAttr = getDynamoRandomKeyOrHash(":");
      if (typeof item === "number") {
        expressAttrVal[keyAttr] = { N: item };
      } else {
        expressAttrVal[keyAttr] = { S: item };
      }
      valuesVariable.push(keyAttr);
    });

    const filterExpressionValue01 = `(${attrKeyVariale} IN (${valuesVariable.join(", ")}))`;

    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        ...expressAttrVal,
      },
      xExpressionAttributeNames: {
        ...expressAttrName,
      },
      xFilterExpression: filterExpressionValue01,
    };
    return result;
  }

  private operation__filterElemMatch({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: { $in: any[] };
  }): IQueryConditions[] {
    const result: IQueryConditions[] = [];
    attrValues.$in.forEach((term) => {
      const query01 = this.operation__filterContains({ term, fieldName });
      result.push(query01);
    });
    return result;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const expressAttrVal: { [key: string]: string } = {};
    const expressAttrName: { [key: string]: string } = {};
    const filterExpress: string[] = [];

    const attrKeyHash01 = getDynamoRandomKeyOrHash("#");
    expressAttrName[attrKeyHash01] = fieldName;

    attrValues.forEach((item) => {
      const keyAttr = getDynamoRandomKeyOrHash(":");
      expressAttrVal[keyAttr] = item;
      filterExpress.push(`${attrKeyHash01} = ${keyAttr}`);
    });

    const _filterExpression = filterExpress.join(" OR ").trim();
    const _filterExpressionValue = filterExpress.length > 1 ? `(${_filterExpression})` : _filterExpression;

    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        ...expressAttrVal,
      },
      xExpressionAttributeNames: {
        ...expressAttrName,
      },
      xFilterExpression: _filterExpressionValue,
    };
    return result;
  }

  private operation__filterNestedMatchObject({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    /*
      ---- Samples ----
      { amount: {$gte: 99} }
      { amount: {$in: [99, 69, 69]} }
    */
    attrValues: Record<string, Record<string, any> | string | number | string[] | number[]>;
  }): IQueryConditions {
    const parentHashKey = getDynamoRandomKeyOrHash("#");
    const xFilterExpressionList: string[] = [];

    const resultQuery: IQueryConditions = {
      xExpressionAttributeValues: {},
      xExpressionAttributeNames: {
        [parentHashKey]: fieldName,
      },
      xFilterExpression: "",
    };

    Object.entries(attrValues).forEach(([subFieldName, queryval]) => {
      //
      let queryValue001: Record<string, any>;
      const childKeyHash = getDynamoRandomKeyOrHash("#");

      resultQuery.xExpressionAttributeNames[childKeyHash] = subFieldName;

      if (queryval && typeof queryval === "object") {
        queryValue001 = { ...queryval };
      } else {
        queryValue001 = { $eq: queryval };
      }

      Object.entries(queryValue001).forEach(([condKey, conditionValue]) => {
        //
        const conditionKey = condKey as keyof typeof QUERY_CONDITION_MAP_NESTED;
        //
        if (!Object.keys(QUERY_CONDITION_MAP_NESTED).includes(conditionKey)) {
          throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
            `Invalid query key: ${conditionKey} @ NestedMatchObject`,
          );
        }
        const conditionExpr = QUERY_CONDITION_MAP_NESTED[conditionKey];
        //
        const attrValueHashKey = getDynamoRandomKeyOrHash(":");
        //
        if (conditionExpr) {
          resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push([`${parentHashKey}.${childKeyHash}`, conditionExpr, attrValueHashKey].join(" "));
          //
        } else if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const fromKey = getDynamoRandomKeyOrHash(":");
          const toKey = getDynamoRandomKeyOrHash(":");

          const [fromVal, toVal] = conditionValue;

          resultQuery.xExpressionAttributeValues[fromKey] = fromVal;
          resultQuery.xExpressionAttributeValues[toKey] = toVal;
          xFilterExpressionList.push([`${parentHashKey}.${childKeyHash}`, "between", fromKey, "and", toKey].join(" "));
          //
        } else if (conditionKey === "$beginsWith") {
          //
          resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push(`begins_with (${parentHashKey}.${childKeyHash}, ${attrValueHashKey})`);
          //
        } else if (conditionKey === "$contains") {
          //
          resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push(`contains (${parentHashKey}.${childKeyHash}, ${attrValueHashKey})`);
          //
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);
          if (String(conditionValue) === "true") {
            xFilterExpressionList.push(`attribute_exists (${parentHashKey}.${childKeyHash})`);
          } else {
            xFilterExpressionList.push(`attribute_not_exists (${parentHashKey}.${childKeyHash})`);
          }
        } else if (conditionKey === "$in" || conditionKey === "$nin") {
          if (conditionKey === "$nin") {
            QueryValidatorCheck.notIn(conditionValue);
          } else {
            QueryValidatorCheck.in_query(conditionValue);
          }

          const attrValues: string[] = [...conditionValue];
          const filterExpress: string[] = [];

          attrValues.forEach((item) => {
            const keyAttr = getDynamoRandomKeyOrHash(":");
            resultQuery.xExpressionAttributeValues[keyAttr] = item;
            filterExpress.push(`${parentHashKey}.${childKeyHash} = ${keyAttr}`);
          });

          const filterExpression01 = filterExpress
            .map((f, _, arr) => {
              if (arr.length > 1) return `(${f})`;
              return f;
            })
            .join(" OR ")
            .trim();

          if (conditionKey === "$nin") {
            xFilterExpressionList.push(`NOT (${filterExpression01})`);
          } else {
            xFilterExpressionList.push(filterExpression01);
          }
        } else {
          throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
            `Nested Query key: ${conditionKey}, not currently supported`,
          );
        }
      });
    });

    const xFilterExpression = xFilterExpressionList
      .map((f, _, arr) => {
        if (arr.length > 1) return `(${f})`;
        return f;
      })
      .join(" AND ");

    resultQuery.xFilterExpression = xFilterExpression;

    LoggingService.log(JSON.stringify({ queryNested: resultQuery }, null, 2));
    return resultQuery;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const attrKeyHash = getDynamoRandomKeyOrHash("#");
    const valueHash = getDynamoRandomKeyOrHash(":");
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [valueHash]: term,
      },
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `contains (${attrKeyHash}, ${valueHash})`,
    };
    return result;
  }

  private operation__filterNot({
    fieldName,
    selectorValues,
  }: {
    fieldName: string;
    selectorValues: any;
  }): IQueryConditions[] {
    //
    const selector: Record<keyof IMocodyKeyConditionParams, any> = { ...selectorValues };

    const mConditions: IQueryConditions[] = [];

    Object.entries(selector).forEach(([conditionKey, conditionValue]) => {
      if (hasQueryConditionValue(conditionKey)) {
        const _conditionKey01 = conditionKey as keyof IMocodyKeyConditionParams;

        if (_conditionKey01 === "$beginsWith") {
          QueryValidatorCheck.beginWith(conditionValue);

          const _queryConditions = this.operation__filterBeginsWith({
            fieldName: fieldName,
            term: conditionValue,
          });
          mConditions.push(_queryConditions);
        } else if (_conditionKey01 === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const [from, to] = conditionValue;
          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from,
            to,
          });
          mConditions.push(_queryConditions);
        } else {
          const conditionExpr = QUERY_CONDITION_MAP_FULL[conditionKey];
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
    return mConditions;
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
    const attrKeyHash01 = getDynamoRandomKeyOrHash("#");
    const fromValueHash = getDynamoRandomKeyOrHash(":");
    const toValueHash = getDynamoRandomKeyOrHash(":");
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [fromValueHash]: from,
        [toValueHash]: to,
      },
      xExpressionAttributeNames: {
        [attrKeyHash01]: fieldName,
      },
      xFilterExpression: [attrKeyHash01, "between", fromValueHash, "and", toValueHash].join(" "),
    };
    return result;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const attrKeyHash01 = getDynamoRandomKeyOrHash("#");
    const valueHash01 = getDynamoRandomKeyOrHash(":");
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [valueHash01]: term,
      },
      xExpressionAttributeNames: {
        [attrKeyHash01]: fieldName,
      },
      xFilterExpression: `begins_with (${attrKeyHash01}, ${valueHash01})`,
    };
    return result;
  }

  private operation__translateAdvancedQueryOperation({
    fieldName,
    queryObject,
  }: {
    fieldName: string;
    queryObject: Record<string, any>;
  }) {
    const queryConditions: IQueryConditions[] = [];
    const notConditions: IQueryConditions[] = [];
    const orConditions: IQueryConditions[] = [];
    //
    Object.entries(queryObject).forEach(([condKey, conditionValue]) => {
      const conditionKey = condKey as keyof IMocodyQueryConditionParams;
      if (conditionValue !== undefined) {
        if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const [from, to] = conditionValue;
          const _queryConditions = this.operation__filterBetween({
            fieldName: fieldName,
            from,
            to,
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
        } else if (conditionKey === "$in") {
          QueryValidatorCheck.in_query(conditionValue);
          const _queryConditions = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$nin") {
          QueryValidatorCheck.notIn(conditionValue);
          const queryConditions01 = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });

          queryConditions01.xFilterExpression = queryConditions01.xFilterExpression.trim().startsWith("(")
            ? `NOT ${queryConditions01.xFilterExpression}`
            : `NOT (${queryConditions01.xFilterExpression})`;

          queryConditions.push(queryConditions01);
        } else if (conditionKey === "$elemMatch") {
          QueryValidatorCheck.elemMatch(conditionValue);
          const elemMatchConditions = this.operation__filterElemMatch({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (elemMatchConditions?.length) {
            elemMatchConditions.forEach((cond) => {
              orConditions.push(cond);
            });
          }
        } else if (conditionKey === "$nestedMatch") {
          QueryValidatorCheck.nestedMatch(conditionValue);
          const nestedMatchConditions = this.operation__filterNestedMatchObject({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (nestedMatchConditions) {
            queryConditions.push(nestedMatchConditions);
          }
        } else if (conditionKey === "$not") {
          QueryValidatorCheck.not_query(conditionValue);
          const _queryConditions = this.operation__filterNot({
            fieldName: fieldName,
            selectorValues: conditionValue,
          });
          if (_queryConditions?.length) {
            _queryConditions.forEach((cond) => {
              notConditions.push(cond);
            });
          }
        } else if (conditionKey === "$notContains") {
          QueryValidatorCheck.notContains(conditionValue);
          const _queryConditions = this.operation__filterContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          _queryConditions.xFilterExpression = `NOT (${_queryConditions.xFilterExpression})`;
          queryConditions.push(_queryConditions);
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
          const conditionExpr = QUERY_CONDITION_MAP_FULL[conditionKey];
          if (hasQueryConditionValue(conditionKey) && conditionExpr) {
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
    LoggingService.log(queryConditions);
    return { queryConditions, notConditions, orConditions };
  }

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const _queryConditions = this.operation__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      conditionExpr: "=",
    });
    return _queryConditions;
  }

  processQueryFilter({
    queryDefs,
    projectionFields,
  }: {
    queryDefs: IMocodyQueryDefinition<any>["query"];
    projectionFields: any[] | undefined | null;
  }) {
    let AND_queryConditions: IQueryConditions[] = [];
    let OR_queryConditions: IQueryConditions[] = [];
    const OR_queryConditions_multiFields: IQueryConditions[][] = [];
    let NOT_queryConditions: IQueryConditions[] = [];
    let NOT_inside_OR_queryConditions: IQueryConditions[] = [];
    //
    const AND_FilterExpressionArray: string[] = [];
    const OR_FilterExpressionArray: string[] = [];
    const OR_FilterExpressionMultiFieldsArray: string[][] = [];
    const NOT_FilterExpressionArray: string[] = [];
    const NOT_inside_OR_FilterExpressionArray: string[] = [];

    Object.entries(queryDefs).forEach(([conditionKey, conditionValue]) => {
      if (conditionKey === "$or") {
        QueryValidatorCheck.or_query(conditionValue);

        const orArray = conditionValue as { [k: string]: any }[];

        orArray.forEach((orQuery) => {
          const OR_queryMultiConditionsPrivate: IQueryConditions[] = [];

          Object.entries(orQuery).forEach(([fieldName, orQueryObjectOrValue], _, arr) => {
            const hasMultiField = arr.length > 1;

            if (orQueryObjectOrValue !== undefined) {
              if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                const orQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                if (orQueryCond01?.queryConditions?.length) {
                  if (hasMultiField) {
                    OR_queryMultiConditionsPrivate.push(...orQueryCond01.queryConditions);
                  } else {
                    OR_queryConditions = [...OR_queryConditions, ...orQueryCond01.queryConditions];
                    NOT_inside_OR_queryConditions = [
                      ...NOT_inside_OR_queryConditions,
                      ...orQueryCond01.notConditions,
                      //
                    ];
                  }
                }
              } else {
                const orQueryCondition01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });
                if (hasMultiField) {
                  OR_queryMultiConditionsPrivate.push(orQueryCondition01);
                } else {
                  OR_queryConditions.push(orQueryCondition01);
                }
              }
            }
          });
          if (OR_queryMultiConditionsPrivate.length) {
            OR_queryConditions_multiFields.push(OR_queryMultiConditionsPrivate);
          }
        });
      } else if (conditionKey === "$and") {
        const andArray = conditionValue as any[];

        QueryValidatorCheck.and_query(andArray);

        andArray.forEach((andQuery) => {
          Object.entries(andQuery).forEach(([fieldName, andQueryObjectOrValue]) => {
            if (andQueryObjectOrValue !== undefined) {
              if (andQueryObjectOrValue && typeof andQueryObjectOrValue === "object") {
                const andQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                AND_queryConditions = [...AND_queryConditions, ...andQueryCond01.queryConditions];
                NOT_queryConditions = [...NOT_queryConditions, ...andQueryCond01.notConditions];
                OR_queryConditions = [...OR_queryConditions, ...andQueryCond01.orConditions];
              } else {
                const andQueryCondition01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                AND_queryConditions = [...AND_queryConditions, andQueryCondition01];
              }
            }
          });
        });
      } else {
        if (conditionKey) {
          if (conditionValue !== undefined) {
            if (conditionValue && typeof conditionValue === "object") {
              const queryCond01 = this.operation__translateAdvancedQueryOperation({
                fieldName: conditionKey,
                queryObject: conditionValue,
              });
              AND_queryConditions = [...AND_queryConditions, ...queryCond01.queryConditions];
              NOT_queryConditions = [...NOT_queryConditions, ...queryCond01.notConditions];
              OR_queryConditions = [...OR_queryConditions, ...queryCond01.orConditions];
            } else {
              const queryCondition01 = this.operation_translateBasicQueryOperation({
                fieldName: conditionKey,
                queryObject: conditionValue,
              });
              AND_queryConditions = [...AND_queryConditions, queryCondition01];
            }
          }
        }
      }
    });

    let _expressionAttributeValues: IDictionaryAttr = {};
    let _expressionAttributeNames: IDictionaryAttr = {};
    let _projectionExpression: string | undefined;
    //

    AND_queryConditions.forEach((item) => {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item.xExpressionAttributeValues,
      };
      AND_FilterExpressionArray.push(item.xFilterExpression);
    });

    OR_queryConditions.forEach((item2) => {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item2.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item2.xExpressionAttributeValues,
      };
      OR_FilterExpressionArray.push(item2.xFilterExpression);
    });

    OR_queryConditions_multiFields.forEach((item2a) => {
      const xFilterExpression: string[] = [];
      item2a.forEach((item01) => {
        _expressionAttributeNames = {
          ..._expressionAttributeNames,
          ...item01.xExpressionAttributeNames,
        };
        _expressionAttributeValues = {
          ..._expressionAttributeValues,
          ...item01.xExpressionAttributeValues,
        };
        xFilterExpression.push(item01.xFilterExpression);
      });
      OR_FilterExpressionMultiFieldsArray.push(xFilterExpression);
    });

    NOT_queryConditions.forEach((item03) => {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item03.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item03.xExpressionAttributeValues,
      };
      NOT_FilterExpressionArray.push(item03.xFilterExpression);
    });

    NOT_inside_OR_queryConditions.forEach((item4) => {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item4.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item4.xExpressionAttributeValues,
      };
      NOT_inside_OR_FilterExpressionArray.push(item4.xFilterExpression);
    });

    let _andfilterExpression: string = "";
    let _orfilterExpression: string = "";
    let _notfilterExpression: string = "";
    let _notInsideOrFilterExpression: string = "";

    if (AND_FilterExpressionArray?.length) {
      _andfilterExpression = AND_FilterExpressionArray.join(" AND ").trim();
    }

    if (OR_FilterExpressionMultiFieldsArray?.length) {
      const subQuery: string[] = [];
      OR_FilterExpressionMultiFieldsArray.forEach((item01) => {
        const val = item01.join(" AND ").trim();
        subQuery.push(`(${val})`);
      });
      _orfilterExpression = [...OR_FilterExpressionArray, ...subQuery].join(" OR ").trim();
      //
    } else if (OR_FilterExpressionArray?.length) {
      _orfilterExpression = OR_FilterExpressionArray.join(" OR ").trim();
    }

    if (NOT_FilterExpressionArray?.length) {
      _notfilterExpression = NOT_FilterExpressionArray.join(" AND ").trim();
      _notfilterExpression = `NOT (${_notfilterExpression})`;
    }

    if (NOT_inside_OR_FilterExpressionArray?.length) {
      _notInsideOrFilterExpression = NOT_inside_OR_FilterExpressionArray.join(" OR ").trim();
      _notInsideOrFilterExpression = `NOT (${_notInsideOrFilterExpression})`;
    }

    let allFilters = [
      _andfilterExpression,
      _notfilterExpression,
      _orfilterExpression,
      _notInsideOrFilterExpression,
    ].filter((f) => f);

    if (allFilters?.length && allFilters.length > 1) {
      allFilters = allFilters.map((f) => `(${f})`);
    }

    const _filterExpression: string = allFilters.join(" AND ");

    if (projectionFields?.length && Array.isArray(projectionFields)) {
      const _projection_expressionAttributeNames: IDictionaryAttr = {};
      projectionFields.forEach((field) => {
        if (field && typeof field === "string") {
          const attrKeyHash = getDynamoRandomKeyOrHash("#");
          _projection_expressionAttributeNames[attrKeyHash] = field;
        }
      });
      _projectionExpression = Object.keys(_projection_expressionAttributeNames).join(", ");
      _expressionAttributeNames = {
        ..._projection_expressionAttributeNames,
        ..._expressionAttributeNames,
      };
    }

    const _expressionAttributeValuesFinal = UtilService.objectHasAnyProperty(_expressionAttributeValues)
      ? _expressionAttributeValues
      : undefined;
    //
    const _expressionAttributeNamesFinal = UtilService.objectHasAnyProperty(_expressionAttributeNames)
      ? _expressionAttributeNames
      : undefined;

    const queryExpressions = {
      expressionAttributeValues: _expressionAttributeValuesFinal,
      filterExpression: _filterExpression,
      projectionExpressionAttr: _projectionExpression,
      expressionAttributeNames: _expressionAttributeNamesFinal,
    };
    LoggingService.log(JSON.stringify({ queryExpressions }, null, 2));
    return queryExpressions;
  }
}

// const result01 = new DynamoFilterQueryOperation().operation__filterNestedMatchObject({
//   fieldName: "user",
//   attrValues: {
//     amount: { $nin: [8999, 7466, 354] },
//     name: { $eq: "chris" },
//   },
// });

// console.log(JSON.stringify(result01, null, 2));
