import { QueryValidatorCheck } from "../helpers/query-validator";
import { LoggingService } from "../helpers/logging-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition } from "../type";
import { MocodyErrorUtilsService } from "../helpers/errors";

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_dynamodb_ExecuteStatement_section.html

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

function hasQueryConditionValue(key: string) {
  if (key && Object.keys(QUERY_CONDITION_MAP_FULL).includes(key) && QUERY_CONDITION_MAP_FULL[key]) {
    return true;
  }
  return false;
}

export class DynamoFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }) {
    return `attribute_exists (${fieldName})`;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }) {
    return `attribute_not_exists (${fieldName})`;
  }

  private operation__helperFilterBasic({
    fieldName,
    val,
    conditionExpr,
  }: {
    fieldName: string;
    conditionExpr: string;
    val: string | number;
  }) {
    return [fieldName, conditionExpr, val].join(" ");
  }

  private operation__filterElemMatch({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    attrValues: { $in: any[] };
  }): string[] {
    const result: string[] = [];
    attrValues.$in.forEach((term) => {
      const query01 = this.operation__filterContains({ term, fieldName });
      result.push(query01);
    });
    return result;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }) {
    return `${fieldName} IN [${attrValues.join(",")}]`;
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
  }): string {
    const parentHashKey = fieldName;
    const xFilterExpressionList: string[] = [];

    Object.entries(attrValues).forEach(([subFieldName, queryval]) => {
      //
      let queryValue001: Record<string, any>;
      const childKeyHash = subFieldName;

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
        const attrValueHashKey = conditionValue;
        //
        if (conditionExpr) {
          // resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push([`${parentHashKey}.${childKeyHash}`, conditionExpr, attrValueHashKey].join(" "));
          //
        } else if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);

          const [fromVal, toVal] = conditionValue;

          xFilterExpressionList.push([`${parentHashKey}.${childKeyHash}`, "BETWEEN", fromVal, "AND", toVal].join(" "));
          //
        } else if (conditionKey === "$beginsWith") {
          //
          xFilterExpressionList.push(`begins_with (${parentHashKey}.${childKeyHash}, ${attrValueHashKey})`);
          //
        } else if (conditionKey === "$contains") {
          //
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
            filterExpress.push(`${parentHashKey}.${childKeyHash} = ${item}`);
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

    return xFilterExpression;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }) {
    return `contains ("${fieldName}", ${term})`;
  }

  private operation__filterNot({ fieldName, selectorValues }: { fieldName: string; selectorValues: any }): string[] {
    //
    const selector: Record<keyof IMocodyKeyConditionParams, any> = { ...selectorValues };

    const mConditions: string[] = [];

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

  private operation__filterBetween({ fieldName, from, to }: { fieldName: string; from: any; to: any }) {
    return `${fieldName} BETWEEN ${from} AND ${to}`;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }) {
    return `begins_with ("${fieldName}", ${term})`;
  }

  private operation__translateAdvancedQueryOperation({
    fieldName,
    queryObject,
  }: {
    fieldName: string;
    queryObject: Record<string, any>;
  }) {
    const queryConditions: string[] = [];
    const notConditions: string[] = [];
    const orConditions: string[] = [];
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

          // queryConditions01.xFilterExpression = queryConditions01.xFilterExpression.trim().startsWith("(")
          //   ? `NOT ${queryConditions01.xFilterExpression}`
          //   : `NOT (${queryConditions01.xFilterExpression})`;

          const queryNotConditions = `NOT (${queryConditions01})`;

          queryConditions.push(queryNotConditions);
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
          const queryCondition01 = this.operation__filterContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          const queryNotConditions = `NOT (${queryCondition01})`;
          queryConditions.push(queryNotConditions);
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
            const queryCondition01 = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            queryConditions.push(queryCondition01);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });
    LoggingService.logAsString({ queryConditions, notConditions, orConditions });
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
    const AND_queryConditions: string[] = [];
    const OR_queryConditions: string[] = [];
    const NOT_queryConditions: string[] = [];
    const NOT_inside_OR_queryConditions: string[] = [];
    const OR_queryConditions_multiFields: string[][] = [];

    Object.entries(queryDefs).forEach(([conditionKey, conditionValue]) => {
      if (conditionKey === "$or") {
        QueryValidatorCheck.or_query(conditionValue);

        const orArray = conditionValue as { [k: string]: any }[];

        orArray.forEach((orQuery) => {
          const OR_queryMultiConditionsPrivate: string[] = [];

          Object.entries(orQuery).forEach(([fieldName, orQueryObjectOrValue], _, arr) => {
            if (orQueryObjectOrValue !== undefined) {
              if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                //
                const hasMultiField = Object.keys(orQueryObjectOrValue).length > 1;

                const orQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                if (orQueryCond01?.queryConditions?.length) {
                  if (hasMultiField) {
                    OR_queryMultiConditionsPrivate.push(...orQueryCond01.queryConditions);
                  } else {
                    OR_queryConditions.push(...orQueryCond01.queryConditions);
                    NOT_inside_OR_queryConditions.push(...orQueryCond01.notConditions);
                  }
                }
              } else {
                const orQueryCondition01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                OR_queryConditions.push(orQueryCondition01);
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
                AND_queryConditions.push(...andQueryCond01.queryConditions);
                NOT_queryConditions.push(...andQueryCond01.notConditions);
                OR_queryConditions.push(...andQueryCond01.orConditions);
              } else {
                const andQueryCondition01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: andQueryObjectOrValue,
                });
                AND_queryConditions.push(andQueryCondition01);
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
              AND_queryConditions.push(...queryCond01.queryConditions);
              NOT_queryConditions.push(...queryCond01.notConditions);
              OR_queryConditions.push(...queryCond01.orConditions);
            } else {
              const queryCondition01 = this.operation_translateBasicQueryOperation({
                fieldName: conditionKey,
                queryObject: conditionValue,
              });
              AND_queryConditions.push(queryCondition01);
            }
          }
        }
      }
    });

    /*
    const AND_queryConditions: string[] = [];
    const OR_queryConditions: string[] = [];
    const NOT_queryConditions: string[] = [];
    const NOT_inside_OR_queryConditions: string[] = [];
    const OR_queryConditions_multiFields: string[][] = [];
    */

    const out01 = {
      AND_queryConditions,
      OR_queryConditions,
      NOT_queryConditions,
      NOT_inside_OR_queryConditions,
      OR_queryConditions_multiFields,
    };

    LoggingService.log({
      AND_queryConditions,
      OR_queryConditions,
      NOT_queryConditions,
      NOT_inside_OR_queryConditions,
      OR_queryConditions_multiFields,
    });

    // const AND_FilterExpressionArray: string[] = [];
    // const OR_FilterExpressionArray: string[] = [];
    // const OR_FilterExpressionMultiFieldsArray: string[][] = [];
    // const NOT_FilterExpressionArray: string[] = [];
    // const NOT_inside_OR_FilterExpressionArray: string[] = [];

    return out01;
  }
}

// const result01 = new DynamoFilterQueryOperation().operation__filterNestedMatchObject({
//   fieldName: "user",
//   attrValues: {
//     amount: { $nin: [8999, 7466, 354] },
//     name: { $eq: "chris" },
//   },
// });

const query = {
  targetId: `caa603bf4b9dcffb715afaf312b480c8`,
  amount: {
    $gte: 0,
  },
  source: {
    $nestedMatch: {
      name: "INVENTORY_OUPUT",
      dataId: { $eq: "inventoryOuputId" },
    },
  },
};

const otherFilter = new DynamoFilterQueryOperation().processQueryFilter({
  queryDefs: query,
  projectionFields: null,
});

console.log({ otherFilter });
