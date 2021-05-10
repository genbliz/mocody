import { UtilService } from "./../helpers/util-service";
import type { DynamoDB, GetItemCommandInput, QueryInput } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { IMocodyPagingResult } from "../type/types";
import { LoggingService } from "../helpers/logging-service";
import { MocodyUtil } from "../helpers/mocody-utils";

export class DynamoQueryScanProcessor {
  //
  async mocody__helperDynamoQueryProcessor<T>({
    evaluationLimit,
    params,
    resultLimit,
    nextPageHash,
    orderDesc,
    index_partitionAndSortKey,
    main_partitionAndSortKey,
    dynamoDb,
    canPaginate,
    featureEntityValue,
    tableFullName,
  }: {
    dynamoDb: () => Promise<DynamoDB>;
    evaluationLimit?: number;
    params: QueryInput;
    resultLimit?: number;
    nextPageHash?: string;
    orderDesc?: boolean;
    canPaginate: boolean;
    featureEntityValue: string;
    tableFullName: string;
    index_partitionAndSortKey: [string, string];
    main_partitionAndSortKey: [string, string];
  }) {
    if (params?.ExpressionAttributeValues) {
      const marshalled = marshall(params.ExpressionAttributeValues, {
        convertEmptyValues: false,
        removeUndefinedValues: true,
      });
      params.ExpressionAttributeValues = marshalled;
    }
    const results = await this.__helperDynamoQueryScanProcessor<T>({
      dynamoDb,
      evaluationLimit,
      params,
      resultLimit,
      nextPageHash,
      orderDesc,
      canPaginate,
      index_partitionAndSortKey,
      main_partitionAndSortKey,
      featureEntityValue,
      tableFullName,
    });
    results.mainResult = this.__unmarshallToJson(results.mainResult);
    return results;
  }

  private async __helperDynamoQueryScanProcessor<T>({
    evaluationLimit,
    params,
    resultLimit,
    nextPageHash,
    orderDesc,
    index_partitionAndSortKey,
    main_partitionAndSortKey,
    dynamoDb,
    canPaginate,
    featureEntityValue,
    tableFullName,
  }: {
    dynamoDb: () => Promise<DynamoDB>;
    evaluationLimit?: number;
    params: QueryInput;
    resultLimit?: number;
    nextPageHash?: string;
    orderDesc?: boolean;
    canPaginate: boolean;
    featureEntityValue: string;
    tableFullName: string;
    index_partitionAndSortKey: [string, string];
    main_partitionAndSortKey: [string, string];
  }) {
    const xDefaultEvaluationLimit = 20;
    const xMinEvaluationLimit = 5;
    const xMaxEvaluationLimit = 500;

    LoggingService.log({
      processorParamsInit: {
        resultLimit,
        orderDesc,
        canPaginate,
        nextPageHash,
        evaluationLimit,
        featureEntityValue,
        index_partitionAndSortKey,
        main_partitionAndSortKey,
        params,
      },
    });

    let returnedItems: any[] = [];
    let evaluationLimit01: number = 0;
    const resultLimit01 = resultLimit || 0;

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

      if (resultLimit01) {
        if (resultLimit01 > evaluationLimit01) {
          evaluationLimit01 = resultLimit01 + 1;
          //
        } else if (resultLimit01 === evaluationLimit01) {
          //
        }
      }
    }

    const params01 = { ...params };

    if (orderDesc === true) {
      params01.ScanIndexForward = false;
    }

    if (evaluationLimit01) {
      params01.Limit = evaluationLimit01;
    }

    if (nextPageHash) {
      const lastEvaluatedKey01 = this.__decodeLastKey(nextPageHash);
      if (lastEvaluatedKey01) {
        params01.ExclusiveStartKey = lastEvaluatedKey01;
      }
    }

    const outResult: IMocodyPagingResult<T[]> = {
      mainResult: [],
      nextPageHash: undefined,
    };

    let hasNext = true;

    const dynamo = await dynamoDb();

    while (hasNext) {
      try {
        const resultDynamo = await dynamo.query(params01);

        params01.ExclusiveStartKey = undefined;

        if (resultDynamo?.Items?.length) {
          returnedItems = [...returnedItems, ...resultDynamo.Items];
        }

        LoggingService.log({ returnedItems__length: returnedItems.length });

        if (resultLimit01 && returnedItems.length >= resultLimit01) {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;

          hasNext = false;

          if (resultDynamo?.LastEvaluatedKey && Object.keys(resultDynamo.LastEvaluatedKey).length) {
            if (canPaginate) {
              outResult.nextPageHash = this.__encodeLastKey(resultDynamo.LastEvaluatedKey);
            }
          }

          const ccns = false;

          if (ccns) {
            if (returnedItems?.length && returnedItems.length > resultLimit01) {
              //
              outResult.mainResult = returnedItems.slice(0, resultLimit01);

              if (canPaginate && outResult?.mainResult?.length) {
                const [lastKeyRawObject] = outResult.mainResult.slice(-1);
                if (lastKeyRawObject) {
                  const customLastEvaluationKey = await this.__createCustomLastEvaluationKey({
                    lastKeyRawObject,
                    featureEntityValue,
                    index_partitionAndSortKey,
                    main_partitionAndSortKey,
                    dynamo,
                    tableFullName,
                  });
                  LoggingService.log({ customLastEvaluationKey });
                  if (customLastEvaluationKey) {
                    outResult.nextPageHash = this.__encodeLastKey(customLastEvaluationKey);
                  }
                }
              }
              break;
            } else if (resultDynamo?.LastEvaluatedKey && Object.keys(resultDynamo.LastEvaluatedKey).length) {
              if (canPaginate) {
                outResult.nextPageHash = this.__encodeLastKey(resultDynamo.LastEvaluatedKey);
              }
            }
          }
        } else if (resultDynamo.LastEvaluatedKey && Object.keys(resultDynamo.LastEvaluatedKey).length) {
          params01.ExclusiveStartKey = resultDynamo.LastEvaluatedKey;
          LoggingService.log({
            LastEvaluatedKey: resultDynamo.LastEvaluatedKey,
            dynamoProcessorParams: params01,
          });
        } else {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;
          hasNext = false;
          break;
        }
      } catch (error) {
        hasNext = false;
        if (returnedItems?.length) {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;
        } else {
          throw error;
        }
      }
    }
    return { ...outResult };
  }

  private __unmarshallToJson(items: any[]) {
    if (items?.length) {
      return items.map((item) => MocodyUtil.mocody_unmarshallToJson(item));
    }
    return items;
  }

  private async __createCustomLastEvaluationKey({
    lastKeyRawObject,
    index_partitionAndSortKey,
    main_partitionAndSortKey,
    dynamo,
    tableFullName,
    featureEntityValue,
  }: {
    lastKeyRawObject: Record<string, any>;
    index_partitionAndSortKey: [string, string];
    main_partitionAndSortKey: [string, string];
    dynamo: DynamoDB;
    tableFullName: string;
    featureEntityValue: string;
  }) {
    if (!(lastKeyRawObject && typeof lastKeyRawObject === "object")) {
      return null;
    }

    const [partitionKeyFieldName, sortKeyFieldName] = main_partitionAndSortKey;
    const [index_PartitionKeyFieldName, index_SortKeyFieldName] = index_partitionAndSortKey;

    /*
     ExclusiveStartKey: {
      createdAtDate: [Object],
      id: [Object],
      featureEntity: [Object],
      featureEntityTenantId: [Object]
    }
    */

    const fields = [
      //
      partitionKeyFieldName,
      sortKeyFieldName,
      index_PartitionKeyFieldName,
      index_SortKeyFieldName,
    ];

    const obj: Record<string, any> = {};
    fields.forEach((key) => {
      if (typeof lastKeyRawObject[key] !== "undefined") {
        obj[key] = lastKeyRawObject[key];
      }
    });

    if (Object.keys(obj).length === 4) {
      return obj;
    }

    const itemJson = MocodyUtil.mocody_unmarshallToJson(lastKeyRawObject);

    const dataId: string | undefined = itemJson?.[partitionKeyFieldName];

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

    if (result.Item && result.Item[partitionKeyFieldName]) {
      const itemObject = { ...result.Item };
      const obj: Record<string, any> = {};
      fields.forEach((key) => {
        if (typeof itemObject[key] !== "undefined") {
          obj[key] = itemObject[key];
        }
      });
      return Object.keys(obj).length === 4 ? obj : null;
    }
    return null;
  }

  private __encodeLastKey(lastEvaluatedKey: Record<string, any>) {
    return UtilService.encodeBase64(JSON.stringify(lastEvaluatedKey));
  }

  private __decodeLastKey(lastKeyHash: string): any {
    try {
      return JSON.parse(UtilService.decodeBase64(lastKeyHash));
    } catch (error) {
      return undefined;
    }
  }
}
