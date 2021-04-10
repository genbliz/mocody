import { UtilService } from "./../helpers/util-service";
import { FuseUtil } from "./../helpers/fuse-utils";
import { RepoModel } from "../model/repo-model";
import type {
  IFuseIndexDefinition,
  IFuseFieldCondition,
  IFuseQueryIndexOptions,
  IFusePagingResult,
  IFuseQueryIndexOptionsNoPaging,
} from "../type/types";
import { FuseErrorUtils, FuseGenericError } from "./../helpers/errors";
import type {
  DynamoDB,
  PutItemInput,
  DeleteItemInput,
  QueryInput,
  BatchGetItemInput,
  AttributeValue,
  BatchGetItemOutput,
  GetItemCommandInput,
} from "@aws-sdk/client-dynamodb";
import Joi from "joi";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { coreSchemaDefinition, IFuseCoreEntityModel } from "../core/base-schema";
import { DynamoManageTable } from "./dynamo-manage-table";
import { LoggingService } from "../helpers/logging-service";
import { FuseInitializerDynamo } from "./dynamo-initializer";
import { DynamoFilterQueryOperation } from "./dynamo-filter-query-operation";
import { DynamoQueryScanProcessor } from "./dynamo-query-scan-processor";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  dynamoDb: () => FuseInitializerDynamo;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

function createTenantSchema(schemaMapDef: Joi.SchemaMap) {
  return Joi.object().keys({
    ...schemaMapDef,
    ...coreSchemaDefinition,
  });
}

type IModelBase = IFuseCoreEntityModel;

export class DynamoDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _fuse_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _fuse_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  private readonly _fuse_featureEntity_Key_Value: { featureEntity: string };

  //
  private readonly _fuse_dynamoDb: () => FuseInitializerDynamo;
  private readonly _fuse_dataKeyGenerator: () => string;
  private readonly _fuse_schema: Joi.Schema;
  private readonly _fuse_tableFullName: string;
  private readonly _fuse_strictRequiredFields: string[];
  private readonly _fuse_featureEntityValue: string;
  private readonly _fuse_secondaryIndexOptions: IFuseIndexDefinition<T>[];
  private readonly _fuse_queryFilter: DynamoFilterQueryOperation;
  private readonly _fuse_queryScanProcessor: DynamoQueryScanProcessor;
  private readonly _fuse_errorHelper: FuseErrorUtils;
  //
  private _fuse_tableManager!: DynamoManageTable<T>;

  constructor({
    schemaDef,
    dynamoDb,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._fuse_dynamoDb = dynamoDb;
    this._fuse_dataKeyGenerator = dataKeyGenerator;
    this._fuse_schema = createTenantSchema(schemaDef);
    this._fuse_tableFullName = baseTableName;
    this._fuse_featureEntityValue = featureEntityValue;
    this._fuse_secondaryIndexOptions = secondaryIndexOptions;
    this._fuse_strictRequiredFields = strictRequiredFields as string[];
    this._fuse_queryFilter = new DynamoFilterQueryOperation();
    this._fuse_queryScanProcessor = new DynamoQueryScanProcessor();
    this._fuse_errorHelper = new FuseErrorUtils();
    this._fuse_featureEntity_Key_Value = { featureEntity: featureEntityValue };
  }

  fuse_tableManager() {
    if (!this._fuse_tableManager) {
      this._fuse_tableManager = new DynamoManageTable<T>({
        dynamoDb: () => this._fuse_dynamoDbInstance(),
        secondaryIndexOptions: this._fuse_secondaryIndexOptions,
        tableFullName: this._fuse_tableFullName,
        partitionKeyFieldName: this._fuse_partitionKeyFieldName,
        sortKeyFieldName: this._fuse_sortKeyFieldName,
      });
    }
    return this._fuse_tableManager;
  }

  private _fuse_dynamoDbInstance(): DynamoDB {
    return this._fuse_dynamoDb().getInstance();
  }

  private _fuse_generateDynamoTableKey() {
    return this._fuse_dataKeyGenerator();
  }

  private _fuse_getLocalVariables() {
    return {
      partitionKeyFieldName: this._fuse_partitionKeyFieldName,
      sortKeyFieldName: this._fuse_sortKeyFieldName,
      //
      featureEntityValue: this._fuse_featureEntityValue,
      //
      tableFullName: this._fuse_tableFullName,
      secondaryIndexOptions: this._fuse_secondaryIndexOptions,
      strictRequiredFields: this._fuse_strictRequiredFields,
    } as const;
  }

  private _fuse_getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._fuse_getLocalVariables();

    const dataMust = {
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _fuse_checkValidateMustBeAnObjectDataType(data: any) {
    if (!data || typeof data !== "object") {
      throw this._fuse_createGenericError(`Data MUST be valid object`);
    }
  }

  private _fuse_checkValidateStrictRequiredFields(onDataObj: any) {
    this._fuse_checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._fuse_getLocalVariables();

    if (strictRequiredFields?.length) {
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._fuse_createGenericError(`Strict required field NOT defined`);
        }
      }
    }
  }

  private _fuse_createGenericError(error: string) {
    return new FuseGenericError(error);
  }

  private _fuse_withConditionPassed({ item, withCondition }: { item: any; withCondition?: IFuseFieldCondition<T> }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  private _fuse_removeDuplicateString<T = string>(strArray: T[]) {
    return Array.from(new Set([...strArray]));
  }

  private async _fuse_allHelpValidateMarshallAndGetValue(data: any) {
    const { error, value } = this._fuse_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this._fuse_errorHelper.fuse_helper_createFriendlyError(msg);
    }

    const marshalledData = FuseUtil.fuse_marshallFromJson(value);
    return await Promise.resolve({
      validatedData: value,
      marshalled: marshalledData,
    });
  }

  async fuse_createOne({ data }: { data: T }) {
    this._fuse_checkValidateStrictRequiredFields(data);

    const { tableFullName, partitionKeyFieldName, featureEntityValue } = this._fuse_getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._fuse_generateDynamoTableKey();
    }

    if (!(dataId && typeof dataId === "string")) {
      throw this._fuse_createGenericError("Invalid dataId generation");
    }

    const dataMust = this._fuse_getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._fuse_createGenericError("FeatureEntity mismatched");
    }

    const { validatedData, marshalled } = await this._fuse_allHelpValidateMarshallAndGetValue(fullData);

    const params: PutItemInput = {
      TableName: tableFullName,
      Item: marshalled,
    };

    await this._fuse_dynamoDbInstance().putItem(params);
    const result: T = validatedData;
    return result;
  }

  async fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T | null> {
    const {
      //
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
      tableFullName,
    } = this._fuse_getLocalVariables();

    this._fuse_errorHelper.fuse_helper_validateRequiredString({
      QueryGetOnePartitionKey: dataId,
      QueryGetOneSortKey: featureEntityValue,
    });

    const params: GetItemCommandInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: { S: dataId },
        [sortKeyFieldName]: { S: featureEntityValue },
      },
    };
    const result = await this._fuse_dynamoDbInstance().getItem(params);
    const item = result.Item as any;
    if (!item) {
      return null;
    }
    const isPassed = this._fuse_withConditionPassed({ withCondition, item });
    if (!isPassed) {
      return null;
    }
    return item;
  }

  async fuse_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IFuseFieldCondition<T>;
  }) {
    this._fuse_checkValidateStrictRequiredFields(updateData);

    const { tableFullName, partitionKeyFieldName, sortKeyFieldName } = this._fuse_getLocalVariables();

    this._fuse_errorHelper.fuse_helper_validateRequiredString({ Update1DataId: dataId });

    const dataInDb = await this.fuse_getOneById({ dataId });

    if (!dataInDb?.[partitionKeyFieldName]) {
      throw this._fuse_errorHelper.fuse_helper_createFriendlyError("Data does NOT exists");
    }
    if (dataInDb?.[sortKeyFieldName] !== this._fuse_featureEntityValue) {
      throw this._fuse_createGenericError("Record does not exists");
    }

    const isPassed = this._fuse_withConditionPassed({
      withCondition,
      item: dataInDb,
    });

    if (!isPassed) {
      throw this._fuse_createGenericError("Update condition failed");
    }

    const dataMust = this._fuse_getBaseObject({ dataId });

    const fullData = {
      ...dataInDb,
      ...updateData,
      ...dataMust,
    };

    const { validatedData, marshalled } = await this._fuse_allHelpValidateMarshallAndGetValue(fullData);

    const params: PutItemInput = {
      TableName: tableFullName,
      Item: marshalled,
    };

    await this._fuse_dynamoDbInstance().putItem(params);
    const result: T = validatedData;
    return result;
  }

  /*
  async fuse_getManyByCondition(paramOptions: IFuseQueryParamOptions<T>) {
    paramOptions.pagingParams = undefined;
    const result = await this.fuse_getManyByConditionPaginate(paramOptions);
    if (result?.mainResult?.length) {
      return result.mainResult;
    }
    return [];
  }

  async fuse_getManyByConditionPaginate(paramOptions: IFuseQueryParamOptions<T>) {
    const { tableFullName, sortKeyFieldName, partitionKeyFieldName } = this._fuse_getLocalVariables();
    //
    if (!paramOptions?.partitionKeyQuery?.equals === undefined) {
      throw this._fuse_createGenericError("Invalid Hash key value");
    }
    if (!sortKeyFieldName) {
      throw this._fuse_createGenericError("Bad query sort configuration");
    }

    let sortKeyQuery: any = {};

    const sortKeyQueryData = paramOptions.sortKeyQuery;
    if (sortKeyQueryData) {
      if (sortKeyQueryData[sortKeyFieldName]) {
        sortKeyQuery = {
          [sortKeyFieldName]: sortKeyQueryData[sortKeyFieldName],
        };
      } else {
        throw this._fuse_createGenericError("Invalid Sort key value");
      }
    }

    const fieldKeys = paramOptions?.fields?.length ? this._fuse_removeDuplicateString(paramOptions.fields) : undefined;

    const filterHashSortKey = this._fuse_queryFilter.fuse__helperDynamoFilterOperation({
      queryDefs: {
        ...sortKeyQuery,
        ...{
          [partitionKeyFieldName]: paramOptions.partitionKeyQuery.equals,
        },
      },
      projectionFields: fieldKeys,
    });
    //
    //
    let otherFilterExpression: string | undefined = undefined;
    let otherExpressionAttributeValues: any = undefined;
    let otherExpressionAttributeNames: any = undefined;
    if (paramOptions?.query) {
      const filterOtherAttr = this._fuse_queryFilter.fuse__helperDynamoFilterOperation({
        queryDefs: paramOptions.query,
        projectionFields: null,
      });

      otherExpressionAttributeValues = filterOtherAttr.expressionAttributeValues;
      otherExpressionAttributeNames = filterOtherAttr.expressionAttributeNames;

      if (filterOtherAttr?.filterExpression && filterOtherAttr?.filterExpression.length > 1) {
        otherFilterExpression = filterOtherAttr.filterExpression;
      }
    }

    const params: QueryInput = {
      TableName: tableFullName,
      KeyConditionExpression: filterHashSortKey.filterExpression,
      ExpressionAttributeValues: {
        ...otherExpressionAttributeValues,
        ...filterHashSortKey.expressionAttributeValues,
      },
      FilterExpression: otherFilterExpression ?? undefined,
      ExpressionAttributeNames: {
        ...otherExpressionAttributeNames,
        ...filterHashSortKey.expressionAttributeNames,
      },
    };

    if (filterHashSortKey?.projectionExpressionAttr) {
      params.ProjectionExpression = filterHashSortKey.projectionExpressionAttr;
    }

    if (paramOptions?.pagingParams?.orderDesc === true) {
      params.ScanIndexForward = false;
    }

    const hashKeyAndSortKey: [string, string] = [partitionKeyFieldName, sortKeyFieldName];

    const paginationObjects = { ...paramOptions.pagingParams };
    const result = await this._fuse_queryScanProcessor.fuse__helperDynamoQueryProcessor<T>({
      dynamoDb: () => this._fuse_dynamoDbInstance(),
      params,
      hashKeyAndSortKey,
      ...paginationObjects,
    });
    return result;
  }
*/

  async fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFuseFieldCondition<T>;
  }) {
    dataIds.forEach((dataId) => {
      this._fuse_errorHelper.fuse_helper_validateRequiredString({
        BatchGetDataId: dataId,
      });
    });

    const originalIds = this._fuse_removeDuplicateString(dataIds);
    const BATCH_SIZE = 80;

    const batchIds: string[][] = [];

    while (originalIds.length > 0) {
      const ids = originalIds.splice(0, BATCH_SIZE);
      batchIds.push(ids);
    }

    LoggingService.log("@fuse_getManyByIds batchIds: ", batchIds.length);

    let result: T[] = [];

    const fieldKeys = fields?.length ? this._fuse_removeDuplicateString(fields) : fields;

    for (const batch of batchIds) {
      const call = await this.fuse_batchGetManyByIdsBasePrivate({
        dataIds: batch,
        fields: fieldKeys,
        withCondition,
      });
      result = [...result, ...call];
    }
    LoggingService.log("@fuse_getManyByIds batchIds result Out: ", result.length);
    return result;
  }

  private async fuse_batchGetManyByIdsBasePrivate({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFuseFieldCondition<T>;
  }) {
    return new Promise<T[]>((resolve, reject) => {
      const getRandom = () =>
        [
          "rand",
          Math.round(Math.random() * 99999),
          Math.round(Math.random() * 88888),
          Math.round(Math.random() * 99),
        ].join("");

      const {
        //
        tableFullName,
        partitionKeyFieldName,
        sortKeyFieldName,
        featureEntityValue,
      } = this._fuse_getLocalVariables();

      const dataIdsNoDup = this._fuse_removeDuplicateString(dataIds);

      type IKey = Record<string, AttributeValue>;

      const getArray: IKey[] = dataIdsNoDup.map((dataId) => {
        const params01 = {
          [partitionKeyFieldName]: { S: dataId },
          [sortKeyFieldName]: { S: featureEntityValue },
        };
        return params01;
      });

      let projectionExpression: string | undefined = undefined;
      let expressionAttributeNames: Record<string, string> | undefined = undefined;

      if (fields?.length) {
        const fieldKeys = this._fuse_removeDuplicateString(fields);
        if (withCondition?.length) {
          /** Add excluded condition */
          withCondition.forEach((condition) => {
            if (!fieldKeys.includes(condition.field)) {
              fieldKeys.push(condition.field);
            }
          });
        }
        expressionAttributeNames = {};
        fieldKeys.forEach((fieldName) => {
          if (typeof fieldName === "string") {
            if (expressionAttributeNames) {
              const attrKeyHash = `#attrKey${getRandom()}k`.toLowerCase();
              expressionAttributeNames[attrKeyHash] = fieldName;
            }
          }
        });
        if (Object.keys(expressionAttributeNames)?.length) {
          projectionExpression = Object.keys(expressionAttributeNames).join(",");
        } else {
          projectionExpression = undefined;
          expressionAttributeNames = undefined;
        }
      }

      const params: BatchGetItemInput = {
        RequestItems: {
          [tableFullName]: {
            Keys: [...getArray],
            ConsistentRead: true,
            ProjectionExpression: projectionExpression,
            ExpressionAttributeNames: expressionAttributeNames,
          },
        },
      };

      let returnedItems: any[] = [];

      const resolveItemResults = (resultItems: any[]) => {
        if (resultItems?.length && withCondition?.length) {
          return resultItems.filter((item) => {
            return withCondition.every((condition) => {
              return item[condition.field] === condition.equals;
            });
          });
        }
        return resultItems || [];
      };

      const batchGetUntilDone = (err: any, data: BatchGetItemOutput | undefined) => {
        if (err) {
          if (returnedItems?.length) {
            resolve(resolveItemResults(returnedItems));
          } else {
            reject(err?.stack);
          }
        } else {
          if (data?.Responses) {
            const itemListRaw = data.Responses[tableFullName];
            if (itemListRaw?.length) {
              const itemList = itemListRaw.map((item) => {
                return FuseUtil.fuse_unmarshallToJson(item);
              });
              returnedItems = [...returnedItems, ...itemList];
            }
          }

          if (data?.UnprocessedKeys && Object.keys(data.UnprocessedKeys).length) {
            const _params: BatchGetItemInput = {
              RequestItems: data.UnprocessedKeys,
            };
            LoggingService.log({ dynamoBatchGetParams: _params });

            this._fuse_dynamoDbInstance().batchGetItem(params, (err, resultData) => {
              batchGetUntilDone(err, resultData);
            });
          } else {
            resolve(resolveItemResults(returnedItems));
          }
        }
      };
      this._fuse_dynamoDbInstance().batchGetItem(params, (err, resultData) => {
        batchGetUntilDone(err, resultData);
      });
    });
  }

  async fuse_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptionsNoPaging<TData, TSortKeyField>,
  ): Promise<T[]> {
    const result = await this._fuse_getManyBySecondaryIndexPaginateBase<TData, TSortKeyField>(paramOption, false);
    if (result?.mainResult) {
      return result.mainResult;
    }
    return [];
  }

  async fuse_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>> {
    return this._fuse_getManyBySecondaryIndexPaginateBase<TData, TSortKeyField>(paramOption, true);
  }

  private async _fuse_getManyBySecondaryIndexPaginateBase<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
    canPaginate: boolean,
  ): Promise<IFusePagingResult<T[]>> {
    const { tableFullName, secondaryIndexOptions } = this._fuse_getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw this._fuse_createGenericError("Invalid secondary index definitions");
    }

    if (!paramOption?.indexName) {
      throw this._fuse_createGenericError("Invalid index name input");
    }

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === paramOption.indexName;
    });

    if (!secondaryIndex) {
      throw this._fuse_createGenericError("Secondary index not named/defined");
    }

    const index_PartitionKeyFieldName = secondaryIndex.partitionKeyFieldName as string;
    const index_SortKeyFieldName = secondaryIndex.sortKeyFieldName as string;

    const partitionSortKeyQuery = paramOption.sortKeyQuery
      ? {
          ...{ [index_SortKeyFieldName]: paramOption.sortKeyQuery },
          ...{ [index_PartitionKeyFieldName]: paramOption.partitionKeyValue },
        }
      : { [index_PartitionKeyFieldName]: paramOption.partitionKeyValue };

    const fieldKeys = paramOption.fields?.length ? this._fuse_removeDuplicateString(paramOption.fields) : undefined;

    const localVariables = this._fuse_getLocalVariables();

    /** Avoid query data leak */
    const hasFeatureEntity = [
      //
      index_PartitionKeyFieldName,
      index_SortKeyFieldName,
    ].includes(localVariables.sortKeyFieldName);

    if (!hasFeatureEntity) {
      paramOption.query = (paramOption.query || {}) as any;

      paramOption.query = {
        ...paramOption.query,
        ...this._fuse_featureEntity_Key_Value,
      } as any;
    } else if (index_PartitionKeyFieldName !== localVariables.sortKeyFieldName) {
      if (localVariables.sortKeyFieldName === index_SortKeyFieldName) {
        partitionSortKeyQuery[index_SortKeyFieldName] = { $eq: localVariables.featureEntityValue as any };
      }
    }

    const mainFilter = this._fuse_queryFilter.processQueryFilter({
      queryDefs: partitionSortKeyQuery,
      projectionFields: fieldKeys,
    });

    let otherFilterExpression: string | undefined = undefined;
    let otherExpressionAttributeValues: any = undefined;
    let otherExpressionAttributeNames: any = undefined;

    if (paramOption.query) {
      const otherFilter = this._fuse_queryFilter.processQueryFilter({
        queryDefs: paramOption.query,
        projectionFields: null,
      });

      otherExpressionAttributeValues = otherFilter.expressionAttributeValues;
      otherExpressionAttributeNames = otherFilter.expressionAttributeNames;

      if (otherFilter?.filterExpression?.length && otherFilter?.filterExpression.length > 1) {
        otherFilterExpression = otherFilter.filterExpression;
      }
    }

    const params: QueryInput = {
      TableName: tableFullName,
      IndexName: paramOption.indexName,
      KeyConditionExpression: mainFilter.filterExpression,
      ExpressionAttributeValues: {
        ...otherExpressionAttributeValues,
        ...mainFilter.expressionAttributeValues,
      },
      FilterExpression: otherFilterExpression ?? undefined,
      ExpressionAttributeNames: {
        ...otherExpressionAttributeNames,
        ...mainFilter.expressionAttributeNames,
      },
    };

    const orderDesc = paramOption?.sort === "desc";

    if (orderDesc) {
      params.ScanIndexForward = false;
    } else {
      params.ScanIndexForward = true;
    }

    if (mainFilter.projectionExpressionAttr) {
      params.ProjectionExpression = mainFilter.projectionExpressionAttr;
    }

    const partitionAndSortKey: [string, string] = [index_PartitionKeyFieldName, index_SortKeyFieldName];

    const result = await this._fuse_queryScanProcessor.fuse__helperDynamoQueryProcessor<T>({
      dynamoDb: () => this._fuse_dynamoDbInstance(),
      params,
      orderDesc,
      partitionAndSortKey,
      evaluationLimit: paramOption.pagingParams?.evaluationLimit,
      nextPageHash: paramOption.pagingParams?.nextPageHash,
      resultLimit: UtilService.isNumberic(paramOption.limit) ? Number(paramOption.limit) : undefined,
      canPaginate,
    });
    return result;
  }

  async fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T> {
    //
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ Del1SortKey: dataId });

    const {
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._fuse_getLocalVariables();

    const dataExist = await this.fuse_getOneById({ dataId, withCondition });

    if (!(dataExist && dataExist[partitionKeyFieldName])) {
      throw this._fuse_errorHelper.fuse_helper_createFriendlyError("Record does NOT exists");
    }

    const params: DeleteItemInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: { S: dataId },
        [sortKeyFieldName]: { S: featureEntityValue },
      },
    };

    try {
      await this._fuse_dynamoDbInstance().deleteItem(params);
    } catch (err) {
      if (err && err.code === "ResourceNotFoundException") {
        throw this._fuse_errorHelper.fuse_helper_createFriendlyError("Table not found");
      } else if (err && err.code === "ResourceInUseException") {
        throw this._fuse_errorHelper.fuse_helper_createFriendlyError("Table in use");
      } else {
        throw err;
      }
    }
    return dataExist;
  }
}
