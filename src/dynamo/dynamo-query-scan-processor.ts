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
    const xDefaultEvaluationLimit = 10;
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

      if (resultLimit) {
        if (resultLimit > evaluationLimit01) {
          evaluationLimit01 = resultLimit + 1;
          //
        } else if (resultLimit === evaluationLimit01) {
          //
        }
      }
    }

    if (orderDesc === true) {
      params.ScanIndexForward = false;
    }

    const params01 = { ...params };

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
      mainResult: returnedItems,
      nextPageHash: undefined,
    };

    let hasNext = true;

    const dynamo = await dynamoDb();

    while (hasNext) {
      try {
        const { Items, LastEvaluatedKey } = await dynamo.query(params01);

        if (Items?.length) {
          returnedItems = [...returnedItems, ...Items];
        }

        if (resultLimit && returnedItems.length >= resultLimit) {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;

          hasNext = false;

          if (index_partitionAndSortKey?.length === 2 && returnedItems.length > resultLimit) {
            //
            outResult.mainResult = returnedItems.slice(0, resultLimit);

            if (canPaginate) {
              const lastKeyRawObject = outResult.mainResult.slice(-1)[0];
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
            if (canPaginate) {
              outResult.nextPageHash = this.__encodeLastKey(LastEvaluatedKey);
            }
          }
        } else if (LastEvaluatedKey && Object.keys(LastEvaluatedKey).length) {
          params01.ExclusiveStartKey = LastEvaluatedKey;
          LoggingService.log({ dynamoProcessorParams: params01 });
        } else {
          outResult.mainResult = returnedItems;
          outResult.nextPageHash = undefined;
          hasNext = false;
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

    const dataId: string | undefined = itemJson[partitionKeyFieldName];

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

  private __decodeLastKey(lastKeyHash: string) {
    let _lastEvaluatedKey: any;
    try {
      const _lastKeyHashStr = UtilService.decodeBase64(lastKeyHash);
      _lastEvaluatedKey = JSON.parse(_lastKeyHashStr);
    } catch (error) {
      _lastEvaluatedKey = undefined;
    }
    return _lastEvaluatedKey;
  }
}
