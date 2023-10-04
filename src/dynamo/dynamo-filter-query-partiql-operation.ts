import { QueryValidatorCheck } from "../helpers/query-validator";
import { LoggingService } from "../helpers/logging-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition } from "../type";
import { MocodyErrorUtilsService } from "../helpers/errors";

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_dynamodb_ExecuteStatement_section.html
//
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-operators.html
// https://repost.aws/questions/QUgNPbBYWiRoOlMsJv-XzrWg/how-to-use-lastevaluatedkey-in-executestatement-request
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
// https://dynobase.dev/dynamodb-partiql/
// https://thomasstep.com/blog/api-pagination-with-dynamodb

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

type IQueryConditions = {
  subStatement: string;
  subParameters: any[];
};

function hasQueryConditionValue(key: string) {
  if (key && Object.keys(QUERY_CONDITION_MAP_FULL).includes(key) && QUERY_CONDITION_MAP_FULL[key]) {
    return true;
  }
  return false;
}

export class DynamoFilterQueryPartiQlOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }) {
    return {
      subStatement: `NOT (${fieldName} is MISSING)`,
      subParameters: [],
    };
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }) {
    return {
      subStatement: `(${fieldName} is MISSING)`,
      subParameters: [],
    };
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
    return {
      subStatement: `${fieldName} ${conditionExpr} ?`,
      subParameters: [val],
    };
  }

  private operation__filterBetween({ fieldName, from, to }: { fieldName: string; from: any; to: any }): IQueryConditions {
    return {
      subStatement: `(${fieldName} BETWEEN ? AND ?)`,
      subParameters: [from, to],
    };
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }) {
    return {
      subStatement: `begins_with(${fieldName}, ?)`,
      subParameters: [term],
    };
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    return {
      subStatement: `contains(${fieldName}, ?)`,
      subParameters: [term],
    };
  }

  private operation__filterNotContains({ fieldName, term }: { fieldName: string; term: string }): IQueryConditions {
    return {
      subStatement: `NOT (contains(${fieldName}, ?))`,
      subParameters: [term],
    };
  }

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const variable01 = attrValues.map(() => "?").join(",");
    return {
      subStatement: `${fieldName} IN [${variable01}]`,
      subParameters: attrValues,
    };
  }

  private operation__filterNotIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const variable01 = attrValues.map(() => "?").join(",");
    return {
      subStatement: `NOT (${fieldName} IN [${variable01}])`,
      subParameters: attrValues,
    };
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

  private operation__filterNestedMatchObject({
    fieldName,
    attrValues,
  }: {
    fieldName: string;
    /**
      ---- Samples ----
      { amount: {$gte: 99} }
      { amount: {$in: [99, 69, 69]} }
    */
    attrValues: Record<string, Record<string, any> | string | number | string[] | number[]>;
  }): IQueryConditions[] {
    const queryList: IQueryConditions[] = [];

    Object.entries(attrValues).forEach(([subFieldName, queryval]) => {
      //
      let queryValue001: Record<string, any>;

      if (queryval && typeof queryval === "object") {
        queryValue001 = { ...queryval };
      } else {
        queryValue001 = { $eq: queryval };
      }

      const fieldNamePath = `${fieldName}.${subFieldName}`;

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
        if (conditionExpr) {
          const query01 = this.operation__helperFilterBasic({
            fieldName: fieldNamePath,
            conditionExpr: conditionExpr,
            val: conditionValue,
          });

          queryList.push(query01);
        } else if (conditionKey === "$between") {
          QueryValidatorCheck.between(conditionValue);

          const [from, to] = conditionValue;

          const query01 = this.operation__filterBetween({
            fieldName: fieldNamePath,
            from,
            to,
          });

          queryList.push(query01);
        } else if (conditionKey === "$beginsWith") {
          const queryCondition01 = this.operation__filterBeginsWith({
            fieldName: fieldNamePath,
            term: conditionValue,
          });

          queryList.push(queryCondition01);
        } else if (conditionKey === "$contains") {
          const query01 = this.operation__filterContains({
            fieldName: fieldNamePath,
            term: conditionValue,
          });

          queryList.push(query01);
        } else if (conditionKey === "$exists") {
          QueryValidatorCheck.exists(conditionValue);

          if (String(conditionValue) === "true") {
            const query01 = this.operation__filterFieldExist({
              fieldName: fieldNamePath,
            });

            queryList.push(query01);
          } else {
            const query01 = this.operation__filterFieldNotExist({
              fieldName: fieldNamePath,
            });
            queryList.push(query01);
          }
        } else if (conditionKey === "$in" || conditionKey === "$nin") {
          if (conditionKey === "$nin") {
            QueryValidatorCheck.notIn(conditionValue);

            const query01 = this.operation__filterNotIn({
              attrValues: conditionValue,
              fieldName: fieldNamePath,
            });

            queryList.push(query01);
          } else {
            QueryValidatorCheck.in_query(conditionValue);

            const query01 = this.operation__filterIn({
              attrValues: conditionValue,
              fieldName: fieldNamePath,
            });

            queryList.push(query01);
          }
        } else {
          throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
            `Nested Query key: ${conditionKey}, not currently supported`,
          );
        }
      });
    });

    return queryList;
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
            fieldName,
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

          const query01 = this.operation__filterNotIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });

          queryConditions.push(query01);
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
          //
          QueryValidatorCheck.nestedMatch(conditionValue);

          const nestedMatchConditions = this.operation__filterNestedMatchObject({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (nestedMatchConditions?.length) {
            queryConditions.push(...nestedMatchConditions);
          }
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
            const quer01 = this.operation__filterFieldExist({
              fieldName: fieldName,
            });
            queryConditions.push(quer01);
          } else if (String(conditionValue) === "false") {
            const quer01 = this.operation__filterFieldNotExist({
              fieldName: fieldName,
            });
            queryConditions.push(quer01);
          }
        } else {
          const conditionExpr = QUERY_CONDITION_MAP_FULL[conditionKey];

          if (hasQueryConditionValue(conditionKey) && conditionExpr) {
            const quer01 = this.operation__helperFilterBasic({
              fieldName: fieldName,
              val: conditionValue,
              conditionExpr: conditionExpr,
            });
            queryConditions.push(quer01);
          } else {
            QueryValidatorCheck.throwQueryNotFound(conditionKey);
          }
        }
      }
    });

    LoggingService.logAsString({
      queryConditions,
      orConditions,
    });

    return {
      queryConditions,
      orConditions,
    };
  }

  private operation_translateBasicQueryOperation({ fieldName, queryObject }: { fieldName: string; queryObject: any }) {
    const query01 = this.operation__helperFilterBasic({
      fieldName: fieldName,
      val: queryObject,
      conditionExpr: "=",
    });
    return query01;
  }

  processQueryFilter({ queryDefs }: { queryDefs: IMocodyQueryDefinition<any>["query"] }) {
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
            if (orQueryObjectOrValue !== undefined) {
              if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                //
                const hasMultiField = Object.keys(orQueryObjectOrValue).length > 1;
                console.log({ hasMultiField, orQueryObjectOrValue });

                const orQueryCond01 = this.operation__translateAdvancedQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                if (orQueryCond01?.queryConditions?.length) {
                  if (hasMultiField) {
                    OR_queryMultiConditionsPrivate.push(...orQueryCond01.queryConditions);
                  } else {
                    OR_queryConditions.push(...orQueryCond01.queryConditions);
                  }
                }
              } else {
                const query01 = this.operation_translateBasicQueryOperation({
                  fieldName,
                  queryObject: orQueryObjectOrValue,
                });

                OR_queryConditions.push(query01);
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

    const out01 = {
      AND_queryConditions,
      OR_queryConditions,
      OR_queryConditions_multiFields,
    };

    LoggingService.logAsString({ out01 });

    const subStatement: string[] = [];
    const subParameter: any[] = [];

    AND_queryConditions.forEach((f, i) => {
      if (i !== 0) {
        subStatement.push(" AND ");
      }
      subStatement.push(f.subStatement);
      subParameter.push(...f.subParameters);
    });

    OR_queryConditions.forEach((f, i) => {
      if (i !== 0) {
        subStatement.push(" OR ");
      }
      subStatement.push(f.subStatement);
      subParameter.push(...f.subParameters);
    });

    console.log({ OR_queryConditions_multiFields });

    if (OR_queryConditions_multiFields?.length) {
      subStatement.push("(");

      OR_queryConditions_multiFields.forEach((qItem, i) => {
        if (i !== 0) {
          subStatement.push(" OR ");
        }

        qItem.forEach((f, i) => {
          if (i !== 0) {
            subStatement.push(" AND ");
          }
          subStatement.push(f.subStatement);
          subParameter.push(...f.subParameters);
        });
      });

      subStatement.push(")");
    }

    return {
      subStatement: subStatement.join(" "),
      subParameter,
    };
  }
}
