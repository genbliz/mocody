import type { DynamoDB, QueryInput, QueryCommandOutput } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { IFusePagingResult } from "../type/types";
import { LoggingService } from "../helpers/logging-service";
import { FuseUtil } from "../helpers/fuse-utils";

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
    dynamoDb: () => DynamoDB;
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

  private __helperDynamoQueryScanProcessor<T>({
    evaluationLimit,
    params,
    resultLimit,
    nextPageHash,
    orderDesc,
    partitionAndSortKey,
    dynamoDb,
    canPaginate,
  }: {
    dynamoDb: () => DynamoDB;
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

    type IResult = QueryCommandOutput | undefined;

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

    return new Promise<IFusePagingResult<T[]>>((resolve, reject) => {
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

      const queryScanUntilDone = (err: any, dataOutput: IResult) => {
        if (err) {
          LoggingService.log(err, err?.stack);
          if (returnedItems?.length) {
            resolve({
              mainResult: returnedItems,
              nextPageHash: undefined,
            });
          } else {
            reject(err.stack);
          }
        } else {
          if (dataOutput?.Items?.length) {
            returnedItems = [...returnedItems, ...dataOutput.Items];
          }

          if (resultLimit && returnedItems.length >= resultLimit) {
            const queryOutputResult: IFusePagingResult<T[]> = {
              mainResult: returnedItems,
              nextPageHash: undefined,
            };

            if (partitionAndSortKey?.length === 2 && returnedItems.length > resultLimit) {
              //
              queryOutputResult.mainResult = returnedItems.slice(0, resultLimit);

              if (canPaginate) {
                const itemObject = queryOutputResult.mainResult.slice(-1)[0];
                const customLastEvaluationKey = this.__createCustomLastEvaluationKey({
                  itemObject,
                  partitionAndSortKey,
                });
                //
                LoggingService.log({ customLastEvaluationKey });
                if (customLastEvaluationKey) {
                  queryOutputResult.nextPageHash = this.__encodeLastKey(customLastEvaluationKey);
                }
              }
            } else if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
              if (canPaginate) {
                queryOutputResult.nextPageHash = this.__encodeLastKey(dataOutput.LastEvaluatedKey);
              }
            }

            resolve(queryOutputResult);
            //
          } else if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
            //
            const paramsDef01 = { ...params };
            //
            paramsDef01.ExclusiveStartKey = dataOutput.LastEvaluatedKey;
            if (evaluationLimit01) {
              paramsDef01.Limit = evaluationLimit01;
            }

            LoggingService.log({ dynamoProcessorParams: paramsDef01 });

            dynamoDb().query(paramsDef01, (err, resultData) => {
              queryScanUntilDone(err, resultData);
            });
          } else {
            resolve({
              mainResult: returnedItems,
              nextPageHash: undefined,
            });
          }
        }
      };

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

      if (orderDesc === true) {
        params01.ScanIndexForward = false;
      }

      LoggingService.log({ dynamoProcessorParams: params01 });
      //
      dynamoDb().query(params01, (err, resultData) => {
        queryScanUntilDone(err, resultData);
      });
    });
  }

  private __unmarshallToJson(items: any[]) {
    if (items?.length) {
      const itemList = items.map((item) => {
        return FuseUtil.mocody_unmarshallToJson(item);
      });
      return itemList;
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
