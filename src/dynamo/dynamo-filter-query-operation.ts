import { QueryValidatorCheck } from "./../helpers/query-validator";
import { LoggingService } from "../helpers/logging-service";
import { UtilService } from "../helpers/util-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition, IQueryNestedArray } from "../type";
import { MocodyErrorUtilsService } from "../helpers/errors";
import { QueryConditionBuilder, getDynamoRandomKeyOrHash } from "./dynamo-helper";

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
  $contains: "",
  $notContains: "",
  $elemMatch: "",
  $nestedMatch: "",
  $nestedArrayMatch: "",
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
  private operation__filterFieldExist({ fieldName }: { fieldName: string }) {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);
    builder.addFilter(`attribute_exists(${hashedFieldName})`);
    return builder.getResult();
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }) {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);
    builder.addFilter(`attribute_not_exists(${hashedFieldName})`);
    return builder.getResult();
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }) {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);
    const hashedValue = builder.addValue(term);

    builder.addFilter(`contains (${hashedFieldName}, ${hashedValue})`);
    return builder.getResult();
  }

  private operation__filterNotContains({ fieldName, term }: { fieldName: string; term: any }) {
    const result01 = this.operation__filterContains({ fieldName, term });
    result01.xFilterExpression = `NOT (${result01.xFilterExpression})`;
    return result01;
  }

  private operation__filterBetween({ fieldName, from, to }: { fieldName: string; from: any; to: any }) {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);

    const hashedValue_from = builder.addValue(from);
    const hashedValue_to = builder.addValue(to);

    const filterExp = [hashedFieldName, "BETWEEN", hashedValue_from, "AND", hashedValue_to].join(" ");

    builder.addFilter(filterExp);
    return builder.getResult();
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);
    const hashedValue = builder.addValue(term);

    builder.addFilter(`begins_with(${hashedFieldName}, ${hashedValue})`);
    return builder.getResult();
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
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);
    const hashedValue = builder.addValue(val);

    const filterExp = [hashedFieldName, conditionExpr, hashedValue].join(" ");

    builder.addFilter(filterExp);
    return builder.getResult();
  }

  private operation__filterNotIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }) {
    const inResult = this.operation__filterIn({ fieldName, attrValues });

    inResult.xFilterExpression = inResult.xFilterExpression.trim().startsWith("(")
      ? `NOT ${inResult.xFilterExpression}`
      : `NOT (${inResult.xFilterExpression})`;
    return inResult;
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }) {
    const builder = new QueryConditionBuilder();
    const hashedFieldName = builder.addName(fieldName);

    attrValues.forEach((item) => {
      const hashedValue = builder.addValue(item);
      const filterExp = `${hashedFieldName} = ${hashedValue}`;
      builder.addFilter(filterExp);
    });

    const result = builder.getResult();
    const rawFilterExpression = builder.getRawFilterExpression();
    result.xFilterExpression = rawFilterExpression.join(" OR ").trim();

    if (rawFilterExpression.length > 1) {
      result.xFilterExpression = `(${result.xFilterExpression})`;
    }

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

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const _queryConditions = this.operation__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      conditionExpr: "=",
    });
    return _queryConditions;
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

      const fieldNamePath = `${parentHashKey}.${childKeyHash}`;

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
          xFilterExpressionList.push([fieldNamePath, conditionExpr, attrValueHashKey].join(" "));
          //
        } else if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const fromKey = getDynamoRandomKeyOrHash(":");
          const toKey = getDynamoRandomKeyOrHash(":");

          const [fromVal, toVal] = conditionValue;

          resultQuery.xExpressionAttributeValues[fromKey] = fromVal;
          resultQuery.xExpressionAttributeValues[toKey] = toVal;
          xFilterExpressionList.push([fieldNamePath, "between", fromKey, "and", toKey].join(" "));
          //
        } else if (conditionKey === "$beginsWith") {
          //
          resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push(`begins_with (${fieldNamePath}, ${attrValueHashKey})`);
          //
        } else if (conditionKey === "$contains") {
          //
          resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
          xFilterExpressionList.push(`contains (${fieldNamePath}, ${attrValueHashKey})`);
          //
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);
          if (String(conditionValue) === "true") {
            xFilterExpressionList.push(`attribute_exists (${fieldNamePath})`);
          } else {
            xFilterExpressionList.push(`attribute_not_exists (${fieldNamePath})`);
          }
        } else if (conditionKey === "$in") {
          QueryValidatorCheck.in_query(conditionValue);

          const attrValues: string[] = [...conditionValue];
          const filterExpress: string[] = [];

          attrValues.forEach((item) => {
            const keyAttr = getDynamoRandomKeyOrHash(":");
            resultQuery.xExpressionAttributeValues[keyAttr] = item;
            filterExpress.push(`${fieldNamePath} = ${keyAttr}`);
          });

          const filterExpression01 = filterExpress
            .map((f, _, arr) => {
              if (arr.length > 1) return `(${f})`;
              return f;
            })
            .join(" OR ")
            .trim();

          xFilterExpressionList.push(filterExpression01);
        } else if (conditionKey === "$nin") {
          QueryValidatorCheck.notIn(conditionValue);

          const attrValues: string[] = [...conditionValue];
          const filterExpress: string[] = [];

          attrValues.forEach((item) => {
            const keyAttr = getDynamoRandomKeyOrHash(":");
            resultQuery.xExpressionAttributeValues[keyAttr] = item;
            filterExpress.push(`${fieldNamePath} = ${keyAttr}`);
          });

          const filterExpression01 = filterExpress
            .map((f, _, arr) => {
              if (arr.length > 1) return `(${f})`;
              return f;
            })
            .join(" OR ")
            .trim();

          xFilterExpressionList.push(`NOT (${filterExpression01})`);
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

    LoggingService.logAsString({ queryNested: resultQuery });
    return resultQuery;
  }

  private operation__filterNestedMatchArray({
    fieldName,
    attrParams,
  }: {
    fieldName: string;
    attrParams: IQueryNestedArray;
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

    const namedPath = attrParams.path.join(".");
    const fieldNamePath = `${parentHashKey}[${attrParams.index}].${namedPath}`;

    Object.entries(attrParams.query).forEach(([condKey, conditionValue]) => {
      //
      const conditionKey = condKey as keyof typeof QUERY_CONDITION_MAP_NESTED;
      //
      if (!Object.keys(QUERY_CONDITION_MAP_NESTED).includes(conditionKey)) {
        throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(`Invalid query key: ${conditionKey} @ NestedMatchArray`);
      }
      const conditionExpr = QUERY_CONDITION_MAP_NESTED[conditionKey];
      //
      const attrValueHashKey = getDynamoRandomKeyOrHash(":");
      //
      if (conditionExpr) {
        resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
        xFilterExpressionList.push([fieldNamePath, conditionExpr, attrValueHashKey].join(" "));
        //
      } else if (conditionKey === "$between") {
        QueryValidatorCheck.between(conditionValue);
        const fromKey = getDynamoRandomKeyOrHash(":");
        const toKey = getDynamoRandomKeyOrHash(":");

        const [fromVal, toVal] = conditionValue;

        resultQuery.xExpressionAttributeValues[fromKey] = fromVal;
        resultQuery.xExpressionAttributeValues[toKey] = toVal;
        xFilterExpressionList.push([fieldNamePath, "between", fromKey, "and", toKey].join(" "));
        //
      } else if (conditionKey === "$beginsWith") {
        //
        resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
        xFilterExpressionList.push(`begins_with (${fieldNamePath}, ${attrValueHashKey})`);
        //
      } else if (conditionKey === "$contains") {
        //
        resultQuery.xExpressionAttributeValues[attrValueHashKey] = conditionValue;
        xFilterExpressionList.push(`contains (${fieldNamePath}, ${attrValueHashKey})`);
        //
      } else if (conditionKey === "$exists") {
        QueryValidatorCheck.exists(conditionValue);
        if (String(conditionValue) === "true") {
          xFilterExpressionList.push(`attribute_exists (${fieldNamePath})`);
        } else {
          xFilterExpressionList.push(`attribute_not_exists (${fieldNamePath})`);
        }
      } else if (conditionKey === "$in") {
        QueryValidatorCheck.in_query(conditionValue);

        const attrValues: string[] = [...conditionValue];
        const filterExpress: string[] = [];

        attrValues.forEach((item) => {
          const keyAttr = getDynamoRandomKeyOrHash(":");
          resultQuery.xExpressionAttributeValues[keyAttr] = item;
          filterExpress.push(`${fieldNamePath} = ${keyAttr}`);
        });

        const filterExpression01 = filterExpress
          .map((f, _, arr) => {
            if (arr.length > 1) return `(${f})`;
            return f;
          })
          .join(" OR ")
          .trim();

        xFilterExpressionList.push(filterExpression01);
      } else if (conditionKey === "$nin") {
        QueryValidatorCheck.notIn(conditionValue);

        const attrValues: string[] = [...conditionValue];
        const filterExpress: string[] = [];

        attrValues.forEach((item) => {
          const keyAttr = getDynamoRandomKeyOrHash(":");
          resultQuery.xExpressionAttributeValues[keyAttr] = item;
          filterExpress.push(`${fieldNamePath} = ${keyAttr}`);
        });

        const filterExpression01 = filterExpress
          .map((f, _, arr) => {
            if (arr.length > 1) return `(${f})`;
            return f;
          })
          .join(" OR ")
          .trim();

        xFilterExpressionList.push(`NOT (${filterExpression01})`);
      } else {
        throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
          `Nested Query key: ${conditionKey}, not currently supported`,
        );
      }
    });

    const xFilterExpression = xFilterExpressionList
      .map((f, _, arr) => {
        if (arr.length > 1) return `(${f})`;
        return f;
      })
      .join(" AND ");

    resultQuery.xFilterExpression = xFilterExpression;

    LoggingService.logAsString({ queryNested: resultQuery });
    return resultQuery;
  }

  private operation__translateAdvancedQueryOperation({
    fieldName,
    queryObject,
  }: {
    fieldName: string;
    queryObject: Record<string, any>;
  }) {
    const queryConditions: IQueryConditions[] = [];
    const orConditions: IQueryConditions[] = [];
    //
    Object.entries(queryObject).forEach(([condKey, conditionValue]) => {
      const conditionKey = condKey as keyof IMocodyQueryConditionParams;
      if (conditionValue !== undefined) {
        if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);
          const [from, to] = conditionValue;

          const query01 = this.operation__filterBetween({
            fieldName: fieldName,
            from,
            to,
          });
          queryConditions.push(query01);
        } else if (conditionKey === "$beginsWith") {
          QueryValidatorCheck.beginWith(conditionValue);

          const query01 = this.operation__filterBeginsWith({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(query01);
        } else if (conditionKey === "$contains") {
          QueryValidatorCheck.contains(conditionValue);

          const query01 = this.operation__filterContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(query01);
        } else if (conditionKey === "$in") {
          QueryValidatorCheck.in_query(conditionValue);

          const query01 = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          queryConditions.push(query01);
        } else if (conditionKey === "$nin") {
          QueryValidatorCheck.notIn(conditionValue);

          const queryConditions01 = this.operation__filterNotIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });

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

          const query01 = this.operation__filterNestedMatchObject({
            fieldName: fieldName,
            attrValues: conditionValue,
          });

          queryConditions.push(query01);
        } else if (conditionKey === "$nestedArrayMatch") {
          QueryValidatorCheck.nestedMatchArray(conditionValue);

          const query01 = this.operation__filterNestedMatchArray({
            fieldName: fieldName,
            attrParams: conditionValue,
          });

          queryConditions.push(query01);
        } else if (conditionKey === "$notContains") {
          QueryValidatorCheck.notContains(conditionValue);

          const query01 = this.operation__filterNotContains({
            fieldName: fieldName,
            term: conditionValue,
          });
          queryConditions.push(query01);
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);

          if (String(conditionValue) === "true") {
            const query01 = this.operation__filterFieldExist({
              fieldName,
            });
            queryConditions.push(query01);
          } else if (String(conditionValue) === "false") {
            const query01 = this.operation__filterFieldNotExist({
              fieldName,
            });
            queryConditions.push(query01);
          }
        } else {
          const conditionExpr = QUERY_CONDITION_MAP_FULL[conditionKey];
          if (hasQueryConditionValue(conditionKey) && conditionExpr) {
            const query01 = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            queryConditions.push(query01);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });
    LoggingService.logAsString({ queryConditions, orConditions });
    return { queryConditions, orConditions };
  }

  processQueryFilter({
    queryDefs,
    projectionFields,
  }: {
    queryDefs: IMocodyQueryDefinition<any>["query"];
    projectionFields: any[] | undefined | null;
  }) {
    const AND_queryConditions: IQueryConditions[] = [];
    const OR_queryConditions: IQueryConditions[] = [];
    const OR_queryConditions_multiFields: IQueryConditions[][] = [];

    Object.entries(queryDefs).forEach(([conditionKey, conditionValue]) => {
      if (conditionKey === "$or") {
        QueryValidatorCheck.or_query(conditionValue);

        const orArray = conditionValue as { [k: string]: any }[];

        orArray.forEach((orQuery) => {
          const OR_queryMultiConditionsPrivate: IQueryConditions[] = [];

          Object.entries(orQuery).forEach(([fieldName, orQueryObjectOrValue], _, arr) => {
            LoggingService.logAsString({ orQuery, fieldName, arr, orQueryObjectOrValue });

            const hasMultiField01 = Object.keys(orQuery).length > 1;

            if (orQueryObjectOrValue !== undefined) {
              if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                //
                const hasMultiField = hasMultiField01 || Object.keys(orQueryObjectOrValue).length > 1;

                const orQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                LoggingService.logAsString({ hasMultiField, orQueryCond01 });

                if (orQueryCond01?.queryConditions?.length) {
                  if (hasMultiField) {
                    OR_queryMultiConditionsPrivate.push(...orQueryCond01.queryConditions);
                  } else {
                    OR_queryConditions.push(...orQueryCond01.queryConditions);
                  }
                }
              } else {
                const orQueryCondition01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                if (hasMultiField01) {
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
          LoggingService.logAsString({ OR_queryMultiConditionsPrivate, OR_queryConditions_multiFields });
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
              const query01 = this.operation__translateAdvancedQueryOperation({
                fieldName: conditionKey,
                queryObject: conditionValue,
              });

              AND_queryConditions.push(...query01.queryConditions);
              OR_queryConditions.push(...query01.orConditions);
            } else {
              const query01 = this.operation_translateBasicQueryOperation({
                fieldName: conditionKey,
                queryObject: conditionValue,
              });
              AND_queryConditions.push(query01);
            }
          }
        }
      }
    });

    let _expressionAttributeValues: IDictionaryAttr = {};
    let _expressionAttributeNames: IDictionaryAttr = {};
    let _projectionExpression: string | undefined;
    //
    const AND_FilterExpressionArray: string[] = [];
    const OR_FilterExpressionArray: string[] = [];
    const OR_FilterExpressionMultiFieldsArray: string[][] = [];
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

    let _andfilterExpression: string = "";
    let _orfilterExpression: string = "";

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

    let allFilters = [_andfilterExpression, _orfilterExpression].filter((f) => f);

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
    LoggingService.logAsString({ queryExpressions });
    return queryExpressions;
  }
}

// const result01 = new DynamoFilterQueryOperation().processQueryFilter({
//   projectionFields: [],
//   queryDefs: {
//     $or: [
//       {
//         category: "syrup",
//         strength: "500mg",
//         name: "paracetamol",
//       },
//       {
//         category: "syrup".toLowerCase().trim(),
//         strength: "500mg".toLowerCase().trim(),
//         name: "paracetamol".toLowerCase().trim(),
//       },
//     ],
//   },
// });

// console.log({ result01 });

// npx ts-node src\dynamo\dynamo-filter-query-operation.ts
