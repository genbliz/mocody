// import { UtilService } from "../helpers/util-service";
import { GetItemCommandInput } from "@aws-sdk/client-dynamodb";
import type { IMocodyPagingResult } from "../type";
import { LoggingService } from "../helpers/logging-service";
import { MocodyInitializerDynamo } from "./dynamo-initializer";
import { ExecuteStatementCommandInput } from "@aws-sdk/lib-dynamodb";

interface IParamInput {
  subStatement: string[];
  subParameter: any[];
}

interface IDynamoQueryPartiqlProcessorParams {
  dynamoDb: () => MocodyInitializerDynamo;
  evaluationLimit?: number;
  params: IParamInput;
  resultLimit?: number | undefined | null;
  nextPageHash?: string | undefined | null;
  sortOrder?: "asc" | "desc" | undefined | null;
  canPaginate: boolean;
  featureEntityValue: string;
  indexName: string;
  tableFullName: string;
  projectionFields: string[] | undefined | null;
  current_partitionValue: string | number;
  current_partitionAndSortKey: [string, string];
  default_partitionAndSortKey: [string, string];
}

export class DynamoQueryPartiqlProcessor {
  //
  async mocody__helperDynamoQueryProcessor<T>({
    evaluationLimit,
    params,
    resultLimit,
    nextPageHash,
    sortOrder,
    dynamoDb,
    canPaginate,
    projectionFields,
    current_partitionValue,
    current_partitionAndSortKey,
    default_partitionAndSortKey,
    featureEntityValue,
    tableFullName,
    indexName,
  }: IDynamoQueryPartiqlProcessorParams) {
    const xDefaultEvaluationLimit = 20;
    const xMinEvaluationLimit = 5;
    const xMaxEvaluationLimit = 500;

    const processorParamsInit = {
      resultLimit,
      sortOrder,
      canPaginate,
      nextPageHash,
      evaluationLimit,
      featureEntityValue,
      current_partitionAndSortKey,
      default_partitionAndSortKey,
      params,
    };

    LoggingService.logAsString({ processorParamsInit });

    let returnedItems: any[] = [];
    let evaluationLimit01: number = 0;
    const pageSize01 = resultLimit || 0;

    if (evaluationLimit) {
      //
      evaluationLimit01 = xDefaultEvaluationLimit;
      if (evaluationLimit) {
        evaluationLimit01 = evaluationLimit;
      }

      if (evaluationLimit01 < xMinEvaluationLimit) {
        evaluationLimit01 = xMinEvaluationLimit;
        //
      } else if (evaluationLimit01 > xMaxEvaluationLimit) {
        evaluationLimit01 = xMaxEvaluationLimit;
      }

      if (pageSize01) {
        if (pageSize01 > evaluationLimit01) {
          evaluationLimit01 = pageSize01 + 1;
          //
        } else if (pageSize01 === evaluationLimit01) {
          evaluationLimit01 = pageSize01 + 2;
        }
      }
    }

    const projectionField01 = (() => {
      if (projectionFields?.length) {
        return projectionFields
          .map((f) => JSON.stringify(f))
          .join(",")
          .trim();
      }
      return "*";
    })();

    const tableName = `"${tableFullName}"."${indexName}"`;

    const [current_PartitionKeyFieldName, current_SortKeyFieldName] = current_partitionAndSortKey;
    if (current_PartitionKeyFieldName) {
      //
    }

    const statementText: string[] = [`SELECT ${projectionField01} FROM ${tableName} WHERE ${current_PartitionKeyFieldName}=?`];

    if (params.subStatement?.length) {
      if (!params.subStatement[0].trim().startsWith("AND")) {
        statementText.push("AND");
      }
      statementText.push(...params.subStatement);
    }

    const parameters: any[] = [current_partitionValue];
    parameters.push(...params.subParameter);

    if (sortOrder === "desc") {
      statementText.push(`ORDER BY ${current_SortKeyFieldName} DESC`);
    } else {
      statementText.push(`ORDER BY ${current_SortKeyFieldName} ASC`);
    }

    LoggingService.logAsString({ statementText });

    const params01: ExecuteStatementCommandInput = {
      Statement: statementText.join(" "),
      NextToken: undefined,
      Parameters: parameters,
    };

    if (evaluationLimit01) {
      params01.Limit = evaluationLimit01;
    }

    if (nextPageHash) {
      // const nextToken01 = this.__decodeLastKey(nextPageHash);
      // if (nextToken01) {
      //   params01.NextToken = nextToken01;
      // }
      params01.NextToken = nextPageHash;
    }

    LoggingService.logAsString({ query_start_params: params01 });

    const outResult: IMocodyPagingResult<T[]> = {
      paginationResults: [],
      nextPageHash: undefined,
    };

    let hasNext = true;

    const dynamo = dynamoDb();
    const itemsLoopedOrderedLength: number[] = [];

    while (hasNext) {
      try {
        const { Items, LastEvaluatedKey, NextToken } = await dynamo.executeStatement(params01);

        params01.NextToken = undefined;

        itemsLoopedOrderedLength.push(Items?.length || 0);

        if (Items?.length) {
          returnedItems = [...returnedItems, ...Items];
        }

        LoggingService.log({
          pageSize01,
          dynamicReturnedItems__length: returnedItems.length,
        });

        if (pageSize01 && pageSize01 >= 1 && returnedItems.length >= pageSize01) {
          const hasMoreResults = returnedItems.length > pageSize01;

          const actualResult01 = hasMoreResults ? returnedItems.slice(0, pageSize01) : [...returnedItems];

          outResult.paginationResults = actualResult01;
          outResult.nextPageHash = undefined;

          LoggingService.log({
            hasMoreResults,
            returnedItems_length: returnedItems.length,
            outResult_paginationResults_length: actualResult01.length,
            canPaginate,
          });

          if (canPaginate) {
            if (hasMoreResults) {
              // const [lastKeyRawObject] = actualResult01.slice(-1);
              // LoggingService.log({ lastKeyRawObject });
              // if (lastKeyRawObject) {
              //   const customLastEvaluationKey = await this.__createCustomLastEvaluationKey({
              //     lastKeyRawObject,
              //     featureEntityValue,
              //     current_partitionAndSortKey,
              //     default_partitionAndSortKey,
              //     dynamo,
              //     tableFullName,
              //   });
              //   LoggingService.logAsString({ customLastEvaluationKey });
              //   if (customLastEvaluationKey) {
              //     outResult.nextPageHash = customLastEvaluationKey;
              //   }
              // }
              outResult.nextPageHash = NextToken;
            } else if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length) {
              outResult.nextPageHash = NextToken;
            }
          }
          hasNext = false;
          break;
        } else if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length && NextToken) {
          params01.NextToken = NextToken;
          LoggingService.logAsString({
            LastEvaluatedKey_RAW: LastEvaluatedKey,
            dynamoProcessorParams: params01,
          });
        } else {
          outResult.paginationResults = [...returnedItems];
          outResult.nextPageHash = undefined;
          hasNext = false;
          break;
        }
      } catch (error) {
        if (returnedItems?.length) {
          outResult.paginationResults = [...returnedItems];
          outResult.nextPageHash = undefined;
        } else {
          throw error;
        }
        hasNext = false;
        break;
      }
    }
    LoggingService.log({
      queryStatistics: {
        canPaginate,
        pageSize01,
        loopCount: itemsLoopedOrderedLength.length,
        itemsLoopedOrderedLength,
        realReturnedItemsCount: returnedItems.length,
        actualReturnedItemsCount: outResult.paginationResults.length,
        nextPageHash: outResult.nextPageHash,
      },
    });
    return { ...outResult };
  }

  //@ts-ignore
  private async __createCustomLastEvaluationKey({
    lastKeyRawObject,
    current_partitionAndSortKey,
    default_partitionAndSortKey,
    dynamo,
    tableFullName,
    featureEntityValue,
  }: {
    lastKeyRawObject: Record<string, any>;
    current_partitionAndSortKey: [string, string];
    default_partitionAndSortKey: [string, string];
    dynamo: MocodyInitializerDynamo;
    tableFullName: string;
    featureEntityValue: string;
  }) {
    if (!(lastKeyRawObject && typeof lastKeyRawObject === "object")) {
      return null;
    }

    const [partitionKeyFieldName, sortKeyFieldName] = default_partitionAndSortKey;
    const [current_PartitionKeyFieldName, current_SortKeyFieldName] = current_partitionAndSortKey;

    const fields01 = [
      //
      partitionKeyFieldName,
      sortKeyFieldName,
      current_PartitionKeyFieldName,
      current_SortKeyFieldName,
    ];

    const fields = Array.from(new Set(fields01));

    const obj: Record<string, any> = {};
    fields.forEach((key) => {
      if (typeof lastKeyRawObject[key] !== "undefined") {
        obj[key] = lastKeyRawObject[key];
      }
    });

    if (Object.keys(obj).length === fields.length) {
      return obj;
    }

    const dataId: string | undefined = lastKeyRawObject?.[partitionKeyFieldName];

    if (!dataId) {
      return null;
    }

    const params01: GetItemCommandInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: { S: dataId },
        [sortKeyFieldName]: { S: featureEntityValue },
      },
    };
    const result = await dynamo.getItem(params01);

    const obj01: Record<string, any> = {};
    if (result.Item && result.Item[partitionKeyFieldName]) {
      const itemObject = { ...result.Item };
      fields.forEach((key) => {
        if (typeof itemObject[key] !== "undefined") {
          obj01[key] = itemObject[key];
        }
      });
    }
    return Object.keys(obj01).length === fields.length ? obj01 : null;
  }

  // private __encodeLastKey(lastEvaluatedKey: Record<string, any>) {
  //   return UtilService.encodeBase64(JSON.stringify(lastEvaluatedKey));
  // }

  // private __decodeLastKey(lastKeyHash: string | undefined): any {
  //   try {
  //     if (!lastKeyHash) {
  //       return undefined;
  //     }
  //     let lastKeyHash01: any = JSON.parse(UtilService.decodeBase64(lastKeyHash));

  //     if (typeof lastKeyHash01 === "string") {
  //       lastKeyHash01 = JSON.parse(lastKeyHash01);
  //     }

  //     if (typeof lastKeyHash01 === "string") {
  //       lastKeyHash01 = JSON.parse(lastKeyHash01);
  //     }

  //     if (typeof lastKeyHash01 === "string") {
  //       lastKeyHash01 = JSON.parse(lastKeyHash01);
  //     }

  //     LoggingService.log({
  //       lastKeyHash,
  //       slastKeyHash_decoded: lastKeyHash01,
  //     });
  //     return lastKeyHash01;
  //   } catch (error) {
  //     LoggingService.error(error);
  //     return undefined;
  //   }
  // }
}
