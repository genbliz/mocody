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

    const params01 = { ...params };

    if (orderDesc === true) {
      params01.ScanIndexForward = false;
    } else {
      params01.ScanIndexForward = true;
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
    const itemsLoopedOrderedLength: number[] = [];

    while (hasNext) {
      try {
        const { Items, LastEvaluatedKey } = await dynamo.query(params01);

        params01.ExclusiveStartKey = undefined;

        itemsLoopedOrderedLength.push(Items?.length || 0);

        if (Items?.length) {
          returnedItems = [...returnedItems, ...Items];
        }

        LoggingService.log({ dynamicReturnedItems__length: returnedItems.length });

        if (pageSize01 && pageSize01 > 1 && returnedItems.length >= pageSize01) {
          const hasMoreResults = returnedItems.length > pageSize01;

          outResult.mainResult = hasMoreResults ? returnedItems.slice(0, pageSize01) : returnedItems;
          outResult.nextPageHash = undefined;

          LoggingService.log({
            hasMoreResults,
            returnedItems_length: returnedItems.length,
            outResult_mainResult_length: outResult.mainResult.length,
          });

          if (canPaginate) {
            if (hasMoreResults) {
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
            } else if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length) {
              outResult.nextPageHash = this.__encodeLastKey(LastEvaluatedKey);
            }
          }
          hasNext = false;
          break;
        } else if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length) {
          params01.ExclusiveStartKey = LastEvaluatedKey;
          LoggingService.log({
            LastEvaluatedKey_RAW: LastEvaluatedKey,
            dynamoProcessorParams: params01,
          });
        } else {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;
          hasNext = false;
          break;
        }
      } catch (error) {
        if (returnedItems?.length) {
          outResult.mainResult = returnedItems;
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
        loopCount: itemsLoopedOrderedLength.length,
        itemsLoopedOrderedLength,
        realReturnedItemsCount: returnedItems.length,
        actualReturnedItemsCount: outResult.mainResult.length,
        nextPageHash: outResult.nextPageHash,
      },
    });
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

  private __decodeLastKey(lastKeyHash: string | undefined): any {
    try {
      if (!lastKeyHash) {
        return undefined;
      }
      let lastKeyHash01: any = JSON.parse(UtilService.decodeBase64(lastKeyHash));

      if (typeof lastKeyHash01 === "string") {
        lastKeyHash01 = JSON.parse(lastKeyHash01);
      }

      if (typeof lastKeyHash01 === "string") {
        lastKeyHash01 = JSON.parse(lastKeyHash01);
      }

      if (typeof lastKeyHash01 === "string") {
        lastKeyHash01 = JSON.parse(lastKeyHash01);
      }

      LoggingService.log({
        lastKeyHash,
        slastKeyHash_decoded: lastKeyHash01,
      });
      return lastKeyHash01;
    } catch (error) {
      LoggingService.error(error);
      return undefined;
    }
  }
}
