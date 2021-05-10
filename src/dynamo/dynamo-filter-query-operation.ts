import { QueryValidatorCheck } from "./../helpers/query-validator";
import { LoggingService } from "../helpers/logging-service";
import { UtilService } from "../helpers/util-service";
import type { IMocodyKeyConditionParams, IMocodyQueryConditionParams, IMocodyQueryDefinition } from "../type/types";
import { MocodyErrorUtilsService } from "../helpers/errors";

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

type FieldPartial<T> = { [P in keyof T]-?: string };
const keyConditionMap: FieldPartial<IMocodyKeyConditionParams> = {
  $eq: "=",
  $lt: "<",
  $lte: "<=",
  $gt: ">",
  $gte: ">=",
  $beginsWith: "",
  $between: "",
};

const conditionMapPre: FieldPartial<Omit<IMocodyQueryConditionParams, keyof IMocodyKeyConditionParams>> = {
  $ne: "<>",
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

type IDictionaryAttr = { [key: string]: any };
type IQueryConditions = {
  xExpressionAttributeValues: IDictionaryAttr;
  xExpressionAttributeNames: IDictionaryAttr;
  xFilterExpression: string;
};

function hasQueryConditionValue(key: string) {
  if (key && Object.keys(conditionMap).includes(key) && conditionMap[key]) {
    return true;
  }
  return false;
}

const getRandom = () =>
  [
    //
    Math.round(Math.random() * 299),
    Math.round(Math.random() * 88),
    Math.round(Math.random() * 777),
  ].join("");

export class DynamoFilterQueryOperation {
  private operation__filterFieldExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = `#r1a${getRandom()}`.toLowerCase();
    const result = {
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `attribute_exists (${attrKeyHash})`,
    } as IQueryConditions;
    return result;
  }

  private operation__filterFieldNotExist({ fieldName }: { fieldName: string }): IQueryConditions {
    const attrKeyHash = `#r2a${getRandom()}`.toLowerCase();
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
    const keyAttr = `:r3a${getRandom()}`.toLowerCase();
    const attrKeyHash = `#r4a${getRandom()}`.toLowerCase();
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

  private operation__filterIn({ fieldName, attrValues }: { fieldName: string; attrValues: any[] }): IQueryConditions {
    const expressAttrVal: { [key: string]: string } = {};
    const expressAttrName: { [key: string]: string } = {};
    const filterExpress: string[] = [];

    const _attrKeyHash = `#r5a${getRandom()}`.toLowerCase();
    expressAttrName[_attrKeyHash] = fieldName;

    attrValues.forEach((item) => {
      const keyAttr = `:r6a${getRandom()}`.toLowerCase();
      expressAttrVal[keyAttr] = item;
      filterExpress.push(`${_attrKeyHash} = ${keyAttr}`);
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
        const conditionKey = condKey as keyof IMocodyKeyConditionParams;
        //
        if (!Object.keys(keyConditionMap).includes(conditionKey)) {
          throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
            `Invalid query key: ${conditionKey} @ NestedMatchObject`,
          );
        }
        const conditionExpr = keyConditionMap[conditionKey];
        //
        const attrValue = `:r7a${getRandom()}`.toLowerCase();
        const attrKeyHash = `#r8a${subFieldName}${getRandom()}`.toLowerCase();
        const parentFieldName = `#r9a${fieldName}${getRandom()}`.toLowerCase();
        //
        if (conditionExpr) {
          const result: IQueryConditions = {
            xExpressionAttributeValues: {
              [attrValue]: val,
            },
            xExpressionAttributeNames: {
              [attrKeyHash]: subFieldName,
              [parentFieldName]: fieldName,
            },
            xFilterExpression: [`${parentFieldName}.${attrKeyHash}`, conditionExpr, attrValue].join(" "),
          };
          results.push(result);
        } else {
          if (conditionKey === "$between") {
            const fromKey = `:r10a${getRandom()}`.toLowerCase();
            const toKey = `:r11a${getRandom()}`.toLowerCase();
            if (!(Array.isArray(val) && val.length === 2)) {
              throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
                "$between query must be an array of length 2",
              );
            }
            const [fromVal, toVal] = val;
            const result: IQueryConditions = {
              xExpressionAttributeValues: {
                [fromKey]: fromVal,
                [toKey]: toVal,
              },
              xExpressionAttributeNames: {
                [attrKeyHash]: subFieldName,
                [parentFieldName]: fieldName,
              },
              xFilterExpression: [`${parentFieldName}.${attrKeyHash}`, "between", fromKey, "and", toKey].join(" "),
            };
            results.push(result);
          } else if (conditionKey === "$beginsWith") {
            const result: IQueryConditions = {
              xExpressionAttributeValues: {
                [attrValue]: val,
              },
              xExpressionAttributeNames: {
                [attrKeyHash]: subFieldName,
                [parentFieldName]: fieldName,
              },
              xFilterExpression: `begins_with (${parentFieldName}.${attrKeyHash}, ${attrValue})`,
            };
            results.push(result);
          } else {
            throw MocodyErrorUtilsService.mocody_helper_createFriendlyError(
              `Query key: ${conditionKey} not currently supported`,
            );
          }
        }
      });
    });
    return results;
  }

  private operation__filterContains({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const attrKeyHash = `#r12a${getRandom()}`.toLowerCase();
    const keyAttr = `:r13a${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: term,
      },
      xExpressionAttributeNames: {
        [attrKeyHash]: fieldName,
      },
      xFilterExpression: `contains (${attrKeyHash}, ${keyAttr})`,
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
          const conditionExpr = conditionMap[conditionKey];
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
    const _attrKeyHash = `#r13a${getRandom()}`.toLowerCase();
    const _fromKey = `:r14a${getRandom()}`.toLowerCase();
    const _toKey = `:r15a${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [_fromKey]: from,
        [_toKey]: to,
      },
      xExpressionAttributeNames: {
        [_attrKeyHash]: fieldName,
      },
      xFilterExpression: [_attrKeyHash, "between", _fromKey, "and", _toKey].join(" "),
    };
    return result;
  }

  private operation__filterBeginsWith({ fieldName, term }: { fieldName: string; term: any }): IQueryConditions {
    const _attrKeyHash = `#r16a${getRandom()}`.toLowerCase();
    const keyAttr = `:r17a${fieldName}${getRandom()}`.toLowerCase();
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        [keyAttr]: term,
      },
      xExpressionAttributeNames: {
        [_attrKeyHash]: fieldName,
      },
      xFilterExpression: `begins_with (${_attrKeyHash}, ${keyAttr})`,
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
          const _queryConditions = this.operation__filterIn({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          _queryConditions.xFilterExpression = `NOT ${_queryConditions.xFilterExpression}`;
          queryConditions.push(_queryConditions);
        } else if (conditionKey === "$elemMatch") {
          QueryValidatorCheck.elemMatch(conditionValue);
          const elemMatchConditions = this.operation__filterElemMatch({
            fieldName: fieldName,
            attrValues: conditionValue,
          });
          if (elemMatchConditions?.length) {
            for (const _queryCondition of elemMatchConditions) {
              orConditions.push(_queryCondition);
            }
          }
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
            selectorValues: conditionValue,
          });
          if (_queryConditions?.length) {
            for (const _queryCondition of _queryConditions) {
              notConditions.push(_queryCondition);
            }
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
          const conditionExpr = conditionMap[conditionKey];
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

    Object.keys(queryDefs).forEach((fieldName_Or_And) => {
      if (fieldName_Or_And === "$or") {
        const orKey = fieldName_Or_And;
        const orArray: any[] = queryDefs[orKey];
        QueryValidatorCheck.or_query(orArray);
        if (orArray && Array.isArray(orArray)) {
          orArray.forEach((orQuery) => {
            const hasMultiField = Object.keys(orQuery || {}).length > 1;
            const OR_queryConditionsPrivate: IQueryConditions[] = [];
            Object.entries(orQuery).forEach(([fieldName, orQueryObjectOrValue]) => {
              LoggingService.log({ orQuery, orQueryObjectOrValue });

              if (orQueryObjectOrValue !== undefined) {
                if (orQueryObjectOrValue && typeof orQueryObjectOrValue === "object") {
                  const _orQueryCond = this.operation__translateAdvancedQueryOperation({
                    fieldName,
                    queryObject: orQueryObjectOrValue,
                  });
                  if (_orQueryCond?.queryConditions?.length) {
                    if (hasMultiField) {
                      OR_queryConditionsPrivate.push(..._orQueryCond.queryConditions);
                    } else {
                      OR_queryConditions = [...OR_queryConditions, ..._orQueryCond.queryConditions];
                      NOT_inside_OR_queryConditions = [
                        ...NOT_inside_OR_queryConditions,
                        ..._orQueryCond.notConditions,
                        //
                      ];
                    }
                  }
                } else {
                  const _orQueryConditions = this.operation_translateBasicQueryOperation({
                    fieldName,
                    queryObject: orQueryObjectOrValue,
                  });
                  if (hasMultiField) {
                    OR_queryConditionsPrivate.push(_orQueryConditions);
                  } else {
                    OR_queryConditions.push(_orQueryConditions);
                  }
                }
              }
            });
            if (OR_queryConditionsPrivate.length) {
              OR_queryConditions_multiFields.push(OR_queryConditionsPrivate);
            }
          });
        }
      } else if (fieldName_Or_And === "$and") {
        const andKey = fieldName_Or_And;
        const andArray: any[] = queryDefs[andKey];
        QueryValidatorCheck.and_query(andArray);
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
                  AND_queryConditions = [...AND_queryConditions, ..._andQueryCond.queryConditions];
                  NOT_queryConditions = [...NOT_queryConditions, ..._andQueryCond.notConditions];
                  OR_queryConditions = [...OR_queryConditions, ..._andQueryCond.orConditions];
                } else {
                  const _andQueryConditions = this.operation_translateBasicQueryOperation({
                    fieldName,
                    queryObject: andQueryObjectOrValue,
                  });
                  AND_queryConditions = [...AND_queryConditions, _andQueryConditions];
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
              AND_queryConditions = [...AND_queryConditions, ..._queryCond.queryConditions];
              NOT_queryConditions = [...NOT_queryConditions, ..._queryCond.notConditions];
              OR_queryConditions = [...OR_queryConditions, ..._queryCond.orConditions];
            } else {
              const _queryConditions = this.operation_translateBasicQueryOperation({
                fieldName: fieldName2,
                queryObject: queryObjectOrValue,
              });
              AND_queryConditions = [...AND_queryConditions, _queryConditions];
            }
          }
        }
      }
    });

    let _expressionAttributeValues: IDictionaryAttr = {};
    let _expressionAttributeNames: IDictionaryAttr = {};
    let _projectionExpression: string | undefined = undefined;
    //

    for (const item of AND_queryConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item.xExpressionAttributeValues,
      };
      AND_FilterExpressionArray.push(item.xFilterExpression);
    }

    for (const item2 of OR_queryConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item2.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item2.xExpressionAttributeValues,
      };
      OR_FilterExpressionArray.push(item2.xFilterExpression);
    }

    for (const item2 of OR_queryConditions_multiFields) {
      const xFilterExpression: string[] = [];
      item2.forEach((item01) => {
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
    }

    for (const item3 of NOT_queryConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item3.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item3.xExpressionAttributeValues,
      };
      NOT_FilterExpressionArray.push(item3.xFilterExpression);
    }

    for (const item4 of NOT_inside_OR_queryConditions) {
      _expressionAttributeNames = {
        ..._expressionAttributeNames,
        ...item4.xExpressionAttributeNames,
      };
      _expressionAttributeValues = {
        ..._expressionAttributeValues,
        ...item4.xExpressionAttributeValues,
      };
      NOT_inside_OR_FilterExpressionArray.push(item4.xFilterExpression);
    }

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
          const attrKeyHash = `#r18a${getRandom()}`.toLowerCase();
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
