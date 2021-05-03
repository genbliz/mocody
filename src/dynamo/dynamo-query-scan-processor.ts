import type { DynamoDB, QueryInput } from "@aws-sdk/client-dynamodb";
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
    partitionAndSortKey,
    dynamoDb,
    canPaginate,
  }: {
    dynamoDb: () => Promise<DynamoDB>;
    evaluationLimit?: number;
    params: QueryInput;
    resultLimit?: number;
    nextPageHash?: string;
    orderDesc?: boolean;
    canPaginate: boolean;
    partitionAndSortKey: [string, string];
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
      partitionAndSortKey,
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
    partitionAndSortKey,
    dynamoDb,
    canPaginate,
  }: {
    dynamoDb: () => Promise<DynamoDB>;
    evaluationLimit?: number;
    params: QueryInput;
    resultLimit?: number;
    nextPageHash?: string;
    orderDesc?: boolean;
    canPaginate: boolean;
    partitionAndSortKey: [string, string];
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
        partitionAndSortKey,
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

          if (partitionAndSortKey?.length === 2 && returnedItems.length > resultLimit) {
            //
            outResult.mainResult = returnedItems.slice(0, resultLimit);

            if (canPaginate) {
              const itemObject = outResult.mainResult.slice(-1)[0];
              const customLastEvaluationKey = this.__createCustomLastEvaluationKey({
                itemObject,
                partitionAndSortKey,
              });
              //
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

  private __createCustomLastEvaluationKey({
    itemObject,
    partitionAndSortKey,
  }: {
    itemObject: Record<string, any>;
    partitionAndSortKey: [string, string];
  }) {
    const obj: Record<string, any> = {};
    partitionAndSortKey.forEach((key) => {
      if (typeof itemObject[key] !== "undefined") {
        obj[key] = itemObject[key];
      }
    });
    return Object.keys(obj).length > 0 ? obj : null;
  }

  private __encodeLastKey(lastEvaluatedKey: Record<string, any>) {
    return Buffer.from(JSON.stringify(lastEvaluatedKey)).toString("base64");
  }

  private __decodeLastKey(lastKeyHash: string) {
    let _lastEvaluatedKey: any;
    try {
      const _lastKeyHashStr = Buffer.from(lastKeyHash, "base64").toString();
      _lastEvaluatedKey = JSON.parse(_lastKeyHashStr);
    } catch (error) {
      _lastEvaluatedKey = undefined;
    }
    return _lastEvaluatedKey;
  }
}
