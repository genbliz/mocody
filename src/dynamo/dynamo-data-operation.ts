import { UtilService } from "./../helpers/util-service";
import { MocodyUtil } from "../helpers/mocody-utils";
import { RepoModel } from "../model";
import type {
  IMocodyIndexDefinition,
  IMocodyFieldCondition,
  IMocodyQueryIndexOptions,
  IMocodyPagingResult,
  IMocodyQueryIndexOptionsNoPaging,
  IMocodyPreparedTransaction,
  IMocodyTransactionPrepare,
  IMocodyQueryDefinition,
} from "../type";
import { MocodyErrorUtils, MocodyGenericError } from "./../helpers/errors";
import {
  PutItemCommandInput,
  DeleteItemCommandInput,
  QueryCommandInput,
  AttributeValue,
  GetItemCommandInput,
  TransactWriteItemsCommandInput,
  BatchGetItemCommandInput,
} from "@aws-sdk/client-dynamodb";
import Joi from "joi";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { coreSchemaDefinition, IMocodyCoreEntityModel } from "../core/base-schema";
import { DynamoManageTable } from "./dynamo-manage-table";
import { LoggingService } from "../helpers/logging-service";
import { MocodyInitializerDynamo } from "./dynamo-initializer";
import { DynamoFilterQueryOperation } from "./dynamo-filter-query-operation";
import { DynamoQueryScanProcessor } from "./dynamo-query-scan-processor";
import lodash from "lodash";
import { getDynamoRandomKeyOrHash } from "./dynamo-helper";
import { DynamoQueryPartiqlProcessor } from "./dynamo-query-partiql-processor";
import { DynamoFilterQueryPartiQlOperation } from "./dynamo-filter-query-partiql-operation";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  dynamoDbInitializer: () => MocodyInitializerDynamo;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

export interface IBulkDataDynamoDb {
  [tableName: string]: {
    PutRequest: { Item: { [key: string]: AttributeValue } };
  }[];
}

type IModelBase = IMocodyCoreEntityModel;

export class DynamoDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _mocody_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _mocody_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  private readonly _mocody_featureEntity_Key_Value: { featureEntity: string };
  //
  private readonly _mocody_dynamoDb: () => MocodyInitializerDynamo;
  private readonly _mocody_dataKeyGenerator: () => string;
  private readonly _mocody_schema: Joi.Schema;
  private readonly _mocody_tableFullName: string;
  private readonly _mocody_strictRequiredFields: string[];
  private readonly _mocody_featureEntityValue: string;
  private readonly _mocody_secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  private readonly _mocody_queryFilter: DynamoFilterQueryOperation;
  private readonly _mocody_queryPartiQlFilter: DynamoFilterQueryPartiQlOperation;
  //
  private readonly _mocody_queryScanProcessor: DynamoQueryScanProcessor;
  private readonly _mocody_queryPartiQlProcessor: DynamoQueryPartiqlProcessor;
  private readonly _mocody_errorHelper: MocodyErrorUtils;
  private readonly _mocody_entityFieldsKeySet: Set<keyof T>;
  //
  private _mocody_tableManager!: DynamoManageTable<T>;

  constructor({
    schemaDef,
    dynamoDbInitializer,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._mocody_dynamoDb = dynamoDbInitializer;
    this._mocody_dataKeyGenerator = dataKeyGenerator;
    this._mocody_tableFullName = baseTableName;
    this._mocody_featureEntityValue = featureEntityValue;
    this._mocody_secondaryIndexOptions = secondaryIndexOptions;
    this._mocody_strictRequiredFields = strictRequiredFields as string[];
    this._mocody_queryFilter = new DynamoFilterQueryOperation();
    this._mocody_queryPartiQlFilter = new DynamoFilterQueryPartiQlOperation();
    this._mocody_queryScanProcessor = new DynamoQueryScanProcessor();
    this._mocody_queryPartiQlProcessor = new DynamoQueryPartiqlProcessor();
    this._mocody_errorHelper = new MocodyErrorUtils();
    this._mocody_featureEntity_Key_Value = { featureEntity: featureEntityValue };
    this._mocody_entityFieldsKeySet = new Set();

    const fullSchemaMapDef = {
      ...schemaDef,
      ...coreSchemaDefinition,
    };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._mocody_entityFieldsKeySet.add(key as keyof T);
    });

    this._mocody_schema = Joi.object().keys(fullSchemaMapDef);
  }

  mocody_tableManager() {
    if (!this._mocody_tableManager) {
      this._mocody_tableManager = new DynamoManageTable<T>({
        dynamoDb: () => this._mocody_dynamoDb(),
        secondaryIndexOptions: this._mocody_secondaryIndexOptions,
        tableFullName: this._mocody_tableFullName,
        partitionKeyFieldName: this._mocody_partitionKeyFieldName,
        sortKeyFieldName: this._mocody_sortKeyFieldName,
      });
    }
    return this._mocody_tableManager;
  }

  private _mocody_dynamoInit() {
    return this._mocody_dynamoDb();
  }

  private _mocody_generateDynamoTableKey() {
    return this._mocody_dataKeyGenerator();
  }

  private _mocody_getLocalVariables() {
    return {
      partitionKeyFieldName: this._mocody_partitionKeyFieldName,
      sortKeyFieldName: this._mocody_sortKeyFieldName,
      //
      featureEntityValue: this._mocody_featureEntityValue,
      //
      tableFullName: this._mocody_tableFullName,
      secondaryIndexOptions: this._mocody_secondaryIndexOptions,
      strictRequiredFields: this._mocody_strictRequiredFields,
    } as const;
  }

  private _mocody_getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._mocody_getLocalVariables();

    const dataMust = {
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _mocody_checkValidateMustBeAnObjectDataType(data: unknown) {
    if (!data || typeof data !== "object") {
      throw this._mocody_createGenericError(`Data MUST be valid object`);
    }
  }

  private _mocody_checkValidateStrictRequiredFields(onDataObj: any) {
    this._mocody_checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._mocody_getLocalVariables();

    if (strictRequiredFields?.length) {
      strictRequiredFields.forEach((field) => {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._mocody_createGenericError(`Strict required field: '${field}', NOT defined`);
        }
      });
    }
  }

  private _mocody_createGenericError(error: string) {
    return new MocodyGenericError(error);
  }

  private _mocody_withConditionPassed({
    item,
    withCondition,
  }: {
    item: any;
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  private _mocody_removeDuplicateString<T = string>(strArray: T[]) {
    return Array.from(new Set([...strArray]));
  }

  private async _mocody_allHelpValidateGetValue(data: any) {
    const { error, value } = this._mocody_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this._mocody_errorHelper.mocody_helper_createFriendlyError(msg);
    }

    return await Promise.resolve({ validatedData: value });
  }

  private _mocody_formatTTL(fullData: IMocodyCoreEntityModel & T) {
    if (fullData?.dangerouslyExpireAt) {
      fullData.dangerouslyExpireAtTTL = UtilService.getEpochTime(fullData.dangerouslyExpireAt);
    } else {
      delete fullData.dangerouslyExpireAtTTL;
    }
    return fullData;
  }

  async mocody_createOne({ data, fieldAliases }: { data: T; fieldAliases?: [keyof T, keyof T][] | undefined | null }) {
    const { tableFullName, partitionKeyFieldName, featureEntityValue } = this._mocody_getLocalVariables();

    try {
      const { marshalled, validatedData } = await this._mocody_validateReady({ data });

      const query01: IMocodyQueryDefinition<IMocodyCoreEntityModel> = {
        [partitionKeyFieldName]: { $exists: false },
      };

      MocodyUtil.validateFieldAlias({
        fieldAliases,
        data: validatedData,
        featureEntity: featureEntityValue,
      });

      const {
        //
        expressionAttributeNames,
        expressionAttributeValues,
        filterExpression,
      } = this._mocody_queryFilter.processQueryFilter({
        queryDefs: query01,
        projectionFields: undefined,
      });

      const params: PutItemCommandInput = {
        TableName: tableFullName,
        Item: marshalled,
        ConditionExpression: filterExpression,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
      };

      await this._mocody_dynamoInit().putItem(params);
      const result: T = { ...validatedData };
      return result;
    } catch (error: any) {
      if (error?.name === "ConditionalCheckFailedException") {
        throw this._mocody_errorHelper.mocody_helper_createFriendlyError(
          `Field, ${partitionKeyFieldName} already exists`,
        );
      }
      throw error;
    }
  }

  async mocody_validateFormatData({ data }: { data: T }): Promise<string> {
    const { marshalled } = await this._mocody_validateReady({ data });
    return JSON.stringify(marshalled);
  }

  async mocody_formatForDump({ dataList }: { dataList: T[] }): Promise<string[]> {
    const bulkData: string[] = [];
    for (const data of dataList) {
      const { marshalled } = await this._mocody_validateReady({ data });
      bulkData.push(JSON.stringify({ Item: marshalled }));
    }
    return bulkData;
  }

  private async _mocody_validateReady({ data }: { data: T }) {
    const { partitionKeyFieldName, featureEntityValue } = this._mocody_getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._mocody_generateDynamoTableKey();
    }

    if (!(dataId && typeof dataId === "string")) {
      throw this._mocody_createGenericError("Invalid dataId generation");
    }

    const dataMust = this._mocody_getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust } as IMocodyCoreEntityModel & T;

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._mocody_createGenericError("FeatureEntity mismatched");
    }

    const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);

    this._mocody_checkValidateStrictRequiredFields(validatedData);

    const validatedDataTTL = this._mocody_formatTTL(validatedData);

    const ready = {
      validatedData,
      marshalled: MocodyUtil.marshallFromJson(validatedDataTTL),
    };
    return ready;
  }

  async mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T | null> {
    const {
      //
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
      tableFullName,
    } = this._mocody_getLocalVariables();

    this._mocody_errorHelper.mocody_helper_validateRequiredString({
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
    const result = await this._mocody_dynamoInit().getItem(params);
    const item01 = result.Item as any;
    if (!item01) {
      return null;
    }
    const item: any = MocodyUtil.unmarshallToJson(item01);
    const isPassed = this._mocody_withConditionPassed({ withCondition, item });
    if (!isPassed) {
      return null;
    }
    return item;
  }

  async mocody_updateOne({
    dataId,
    updateData,
    withCondition,
    fieldAliases,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
    fieldAliases?: [keyof T, keyof T][] | undefined | null;
  }) {
    const {
      //
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    this._mocody_errorHelper.mocody_helper_validateRequiredString({ Update1DataId: dataId });

    const dataInDb = await this.mocody_getOneById({ dataId });

    if (!dataInDb?.[partitionKeyFieldName]) {
      throw this._mocody_errorHelper.mocody_helper_createFriendlyError("Data does NOT exists");
    }
    if (dataInDb?.[sortKeyFieldName] !== featureEntityValue) {
      throw this._mocody_createGenericError("Record does not exists");
    }

    const isPassed = this._mocody_withConditionPassed({
      withCondition,
      item: dataInDb,
    });

    if (!isPassed) {
      throw this._mocody_createGenericError("Update condition failed");
    }

    const dataMust = this._mocody_getBaseObject({ dataId });

    const fullData = {
      ...dataInDb,
      ...updateData,
      ...dataMust,
    };

    const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);

    MocodyUtil.validateFieldAlias({
      data: validatedData,
      fieldAliases,
      featureEntity: featureEntityValue,
    });

    this._mocody_checkValidateStrictRequiredFields(validatedData);

    const validatedData01 = this._mocody_formatTTL(validatedData);

    const query01: IMocodyQueryDefinition<IMocodyCoreEntityModel> = {
      [partitionKeyFieldName]: { $exists: true },
    };

    const {
      //
      expressionAttributeNames,
      expressionAttributeValues,
      filterExpression,
    } = this._mocody_queryFilter.processQueryFilter({
      queryDefs: query01,
      projectionFields: undefined,
    });

    const params: PutItemCommandInput = {
      TableName: tableFullName,
      Item: MocodyUtil.marshallFromJson(validatedData01),
      ConditionExpression: filterExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
    };
    await this._mocody_dynamoInit().putItem(params);
    const result: T = validatedData;
    return result;
  }

  async mocody_prepareTransaction({
    transactPrepareInfo,
  }: {
    transactPrepareInfo: IMocodyTransactionPrepare<T>[];
  }): Promise<IMocodyPreparedTransaction[]> {
    const {
      //
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    const prepareTransactData: IMocodyPreparedTransaction[] = [];

    for (const transactInfoItem of transactPrepareInfo) {
      if (transactInfoItem.kind === "create") {
        const { marshalled } = await this._mocody_validateReady({ data: transactInfoItem.data });

        prepareTransactData.push({
          kind: transactInfoItem.kind,
          tableName: tableFullName,
          data: marshalled,
          partitionKeyFieldName,
        });
      } else if (transactInfoItem.kind === "update") {
        const dataMust = this._mocody_getBaseObject({ dataId: transactInfoItem.dataId });

        const fullData = {
          ...transactInfoItem.data,
          ...dataMust,
        };

        const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);
        this._mocody_checkValidateStrictRequiredFields(validatedData);

        const validatedData01 = this._mocody_formatTTL(validatedData);

        prepareTransactData.push({
          partitionKeyFieldName,
          kind: transactInfoItem.kind,
          tableName: tableFullName,
          data: MocodyUtil.marshallFromJson(validatedData01),
          keyQuery: {
            [partitionKeyFieldName]: { S: transactInfoItem.dataId },
            [sortKeyFieldName]: { S: featureEntityValue },
          },
        });
      } else if (transactInfoItem.kind === "delete") {
        prepareTransactData.push({
          partitionKeyFieldName,
          kind: transactInfoItem.kind,
          tableName: tableFullName,
          keyQuery: {
            [partitionKeyFieldName]: { S: transactInfoItem.dataId },
            [sortKeyFieldName]: { S: featureEntityValue },
          },
        });
      }
    }
    return Promise.resolve(prepareTransactData);
  }

  async mocody_executeTransaction({ transactInfo }: { transactInfo: IMocodyPreparedTransaction[] }): Promise<void> {
    const transactData: TransactWriteItemsCommandInput = {
      TransactItems: [],
    };

    for (const item of transactInfo) {
      if (item.kind === "create") {
        const query01: IMocodyQueryDefinition<IMocodyCoreEntityModel> = {
          [item.partitionKeyFieldName]: { $exists: false },
        };

        const {
          //
          expressionAttributeNames,
          expressionAttributeValues,
          filterExpression,
        } = this._mocody_queryFilter.processQueryFilter({
          queryDefs: query01,
          projectionFields: undefined,
        });

        transactData.TransactItems?.push({
          Put: {
            TableName: item.tableName,
            Item: item.data,
            ConditionExpression: filterExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
          },
        });
      } else if (item.kind === "update") {
        const query01: IMocodyQueryDefinition<IMocodyCoreEntityModel> = {
          [item.partitionKeyFieldName]: { $exists: true },
        };

        const {
          //
          expressionAttributeNames,
          expressionAttributeValues,
          filterExpression,
        } = this._mocody_queryFilter.processQueryFilter({
          queryDefs: query01,
          projectionFields: undefined,
        });

        transactData.TransactItems?.push({
          Put: {
            TableName: item.tableName,
            Item: item.data,
            ConditionExpression: filterExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
          },
        });
      } else if (item.kind === "delete") {
        const query01: IMocodyQueryDefinition<IMocodyCoreEntityModel> = {
          [item.partitionKeyFieldName]: { $exists: true },
        };

        const {
          //
          expressionAttributeNames,
          expressionAttributeValues,
          filterExpression,
        } = this._mocody_queryFilter.processQueryFilter({
          queryDefs: query01,
          projectionFields: undefined,
        });

        transactData.TransactItems?.push({
          Delete: {
            TableName: item.tableName,
            Key: item.keyQuery,
            ConditionExpression: filterExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
          },
        });
      }
    }
    await this._mocody_dynamoInit().transactWriteItems(transactData);
  }

  private _mocody_getProjectionFields<TProj = T>({
    excludeFields,
    fields,
  }: {
    excludeFields?: (keyof TProj)[] | undefined | null;
    fields?: (keyof TProj)[] | undefined | null;
  }) {
    return MocodyUtil.getProjectionFields({
      excludeFields,
      fields,
      entityFields: Array.from(this._mocody_entityFieldsKeySet) as any[],
    });
  }

  async mocody_getManyByIds({
    dataIds,
    fields,
    excludeFields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined | null;
    excludeFields?: (keyof T)[] | undefined | null;
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }) {
    dataIds.forEach((dataId) => {
      this._mocody_errorHelper.mocody_helper_validateRequiredString({
        BatchGetDataId: dataId,
      });
    });

    const originalIds = this._mocody_removeDuplicateString(dataIds);
    const BATCH_SIZE = 80;

    const batchIds = lodash.chunk(originalIds, BATCH_SIZE);

    LoggingService.log({
      batchIds,
      "@mocody_getManyByIds batchIds": batchIds.length,
    });

    let resultAll: T[] = [];

    const fieldKeys = this._mocody_getProjectionFields({ fields, excludeFields });

    for (const batch of batchIds) {
      const callByIds = await this.mocody_batchGetManyByIdsBasePrivate({
        dataIds: batch,
        fields: fieldKeys,
        withCondition,
      });
      resultAll = [...resultAll, ...callByIds];
    }
    LoggingService.log("@mocody_getManyByIds batchIds result Out: ", resultAll.length);
    return resultAll;
  }

  private async mocody_batchGetManyByIdsBasePrivate({
    dataIds,
    fields,
    excludeFields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined | null;
    excludeFields?: (keyof T)[] | undefined | null;
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }) {
    const {
      //
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    const dataIdsNoDup = this._mocody_removeDuplicateString(dataIds);

    type IKey = Record<string, AttributeValue>;

    let projectionExpression: string | undefined;
    let expressionAttributeNames: Record<string, string> | undefined;

    const fields01 = this._mocody_getProjectionFields({ fields, excludeFields });

    if (fields01?.length) {
      const fieldKeys = new Set(fields01);
      if (withCondition?.length) {
        /** Add excluded condition */
        withCondition.forEach((condition) => {
          fieldKeys.add(condition.field);
        });
      }
      expressionAttributeNames = {};
      fieldKeys.forEach((fieldName) => {
        if (typeof fieldName === "string") {
          if (expressionAttributeNames) {
            const attrKeyHash = getDynamoRandomKeyOrHash("#");
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

    const getArray: IKey[] = dataIdsNoDup.map((dataId) => {
      const params01 = {
        [partitionKeyFieldName]: { S: dataId },
        [sortKeyFieldName]: { S: featureEntityValue },
      };
      return params01;
    });

    const params: BatchGetItemCommandInput = {
      RequestItems: {
        [tableFullName]: {
          Keys: [...getArray],
          ProjectionExpression: projectionExpression,
          ExpressionAttributeNames: expressionAttributeNames,
        },
      },
    };

    LoggingService.log({ batchGetItemParams: params });

    let hasNext = true;
    let params01 = { ...params };
    let returnedItems: any[] = [];

    const dynamo = this._mocody_dynamoInit();

    LoggingService.log({ fetchForDataIds: dataIds });

    while (hasNext) {
      try {
        const { Responses, UnprocessedKeys } = await dynamo.batchGetItem(params01);

        if (Responses) {
          const itemListRaw = Responses[tableFullName];
          if (itemListRaw?.length) {
            const itemList = itemListRaw.map((item) => MocodyUtil.unmarshallToJson(item));
            returnedItems = [...returnedItems, ...itemList];
          }
        }

        if (UnprocessedKeys && Object.keys(UnprocessedKeys).length) {
          params01 = {
            RequestItems: UnprocessedKeys,
          };
          LoggingService.log({ dynamoBatchGetParams: params01 });
        } else {
          hasNext = false;
          break;
        }
      } catch (error) {
        hasNext = false;
        LoggingService.error(error);
        if (!returnedItems?.length) {
          throw error;
        } else {
          break;
        }
      }
    }

    if (returnedItems?.length && withCondition?.length) {
      const returnedItems01: any[] = [];
      returnedItems.forEach((item) => {
        const canInclude = withCondition.every((condition) => item[condition.field] === condition.equals);
        if (canInclude) {
          returnedItems01.push(item);
        }
      });
      return returnedItems01;
    }
    return returnedItems;
  }

  async mocody_getManyByIndex<TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptionsNoPaging<TData, TSortKeyField>,
  ): Promise<TData[]> {
    const result = await this._mocody_getManyBySecondaryIndexPaginateBase<TData, TData, TSortKeyField>({
      paramOption,
      canPaginate: false,
      enableRelationFetch: false,
    });
    if (result?.paginationResults) {
      return result.paginationResults;
    }
    return [];
  }

  async mocody_getManyByIndexPaginate<TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>> {
    return this._mocody_getManyBySecondaryIndexPaginateBase<TData, TData, TSortKeyField>({
      paramOption,
      canPaginate: true,
      enableRelationFetch: false,
    });
  }

  async mocody_getManyWithRelation<TQuery = T, TData = T, TSortKeyField extends string | number = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TQuery, TSortKeyField>, "pagingParams">,
  ): Promise<TData[]> {
    const result = await this._mocody_getManyBySecondaryIndexPaginateBase<TQuery, TData, TSortKeyField>({
      paramOption,
      canPaginate: false,
      enableRelationFetch: true,
    });
    if (result?.paginationResults) {
      return result.paginationResults;
    }
    return [];
  }

  async mocody_getManyWithRelationPaginate<TQuery = T, TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptions<TQuery, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>> {
    return this._mocody_getManyBySecondaryIndexPaginateBase<TQuery, TData, TSortKeyField>({
      paramOption,
      canPaginate: true,
      enableRelationFetch: true,
    });
  }

  private async _mocody_getManyBySecondaryIndexPaginateBase<TQuery, TData, TSortKeyField extends string | number>({
    paramOption,
    canPaginate,
    enableRelationFetch,
  }: {
    paramOption: IMocodyQueryIndexOptions<TQuery, TSortKeyField>;
    canPaginate: boolean;
    enableRelationFetch: boolean;
  }): Promise<IMocodyPagingResult<TData[]>> {
    const {
      //
      tableFullName,
      secondaryIndexOptions,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw this._mocody_createGenericError("Invalid secondary index definitions");
    }

    const paramOption01 = { ...paramOption };

    if (!paramOption01?.indexName) {
      throw this._mocody_createGenericError("Invalid index name input");
    }

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === paramOption01.indexName;
    });

    if (!secondaryIndex) {
      throw this._mocody_createGenericError(
        `Secondary index '${paramOption01.indexName}' not named/defined in secondaryIndexOptions`,
      );
    }

    let projectionFields: (keyof TQuery)[] | undefined;

    const fields01 = this._mocody_getProjectionFields({
      fields: paramOption01.fields,
      excludeFields: paramOption01.excludeFields,
    });

    if (canPaginate && fields01?.length) {
      const fieldSet01 = new Set(fields01);
      fieldSet01.add(partitionKeyFieldName as any);
      projectionFields = Array.from(fieldSet01);
    } else {
      projectionFields = fields01;
    }

    let evaluationLimit01: number | undefined;
    let resultLimit01: number | undefined;

    if (paramOption01.limit && UtilService.isNumericInteger(paramOption01.limit)) {
      resultLimit01 = Number(paramOption01.limit);
    }

    if (paramOption01?.pagingParams?.evaluationLimit) {
      evaluationLimit01 = paramOption01?.pagingParams?.evaluationLimit;
    } else if (!paramOption01.query && resultLimit01) {
      evaluationLimit01 = resultLimit01;
    }

    const current_PartitionKeyFieldName = secondaryIndex.partitionKeyFieldName as string;
    const current_SortKeyFieldName = secondaryIndex.sortKeyFieldName as string;

    const default_partitionAndSortKey: [string, string] = [partitionKeyFieldName, sortKeyFieldName];
    const current_partitionAndSortKey: [string, string] = [current_PartitionKeyFieldName, current_SortKeyFieldName];

    const partitionSortKeyQuery = paramOption01.sortKeyQuery
      ? {
          ...{ [current_SortKeyFieldName]: paramOption01.sortKeyQuery },
          ...{ [current_PartitionKeyFieldName]: paramOption01.partitionKeyValue },
        }
      : { [current_PartitionKeyFieldName]: paramOption01.partitionKeyValue };

    if (!enableRelationFetch) {
      /** This block avoids query data leak */
      const localVariables = this._mocody_getLocalVariables();

      const hasFeatureEntity = [
        //
        current_PartitionKeyFieldName,
        current_SortKeyFieldName,
      ].includes(localVariables.sortKeyFieldName);

      if (!hasFeatureEntity) {
        paramOption01.query = (paramOption01.query || {}) as any;

        paramOption01.query = {
          ...paramOption01.query,
          ...this._mocody_featureEntity_Key_Value,
        } as any;
      } else if (current_PartitionKeyFieldName !== localVariables.sortKeyFieldName) {
        if (localVariables.sortKeyFieldName === current_SortKeyFieldName) {
          partitionSortKeyQuery[current_SortKeyFieldName] = { $eq: localVariables.featureEntityValue } as any;
        }
      }
    }

    const mainFilter = this._mocody_queryFilter.processQueryFilter({
      queryDefs: partitionSortKeyQuery,
      projectionFields,
    });

    let otherFilterExpression: string | undefined;
    let otherExpressionAttributeValues: any;
    let otherExpressionAttributeNames: any;

    if (paramOption01.query) {
      const otherFilter = this._mocody_queryFilter.processQueryFilter({
        queryDefs: paramOption01.query,
        projectionFields: null,
      });

      otherExpressionAttributeValues = otherFilter.expressionAttributeValues;
      otherExpressionAttributeNames = otherFilter.expressionAttributeNames;

      if (otherFilter?.filterExpression?.length && otherFilter?.filterExpression.length > 1) {
        otherFilterExpression = otherFilter.filterExpression;
      }
    }

    const params: QueryCommandInput = {
      TableName: tableFullName,
      IndexName: paramOption01.indexName,
      KeyConditionExpression: mainFilter.filterExpression,
      FilterExpression: otherFilterExpression ?? undefined,
      ExpressionAttributeValues: {
        ...otherExpressionAttributeValues,
        ...mainFilter.expressionAttributeValues,
      },
      ExpressionAttributeNames: {
        ...otherExpressionAttributeNames,
        ...mainFilter.expressionAttributeNames,
      },
    };

    const orderDesc = paramOption01?.sort === "desc";

    if (orderDesc) {
      params.ScanIndexForward = false;
    } else {
      params.ScanIndexForward = true;
    }

    if (mainFilter.projectionExpressionAttr) {
      params.ProjectionExpression = mainFilter.projectionExpressionAttr;
    }

    let nextPageHash01 = paramOption01?.pagingParams?.nextPageHash;

    if (nextPageHash01 === "undefined") {
      nextPageHash01 = undefined;
    }
    if (nextPageHash01 === "null") {
      nextPageHash01 = undefined;
    }

    const result = await this._mocody_queryScanProcessor.mocody__helperDynamoQueryProcessor<TData>({
      dynamoDb: () => this._mocody_dynamoInit(),
      params,
      orderDesc,
      canPaginate,
      tableFullName,
      featureEntityValue,
      default_partitionAndSortKey,
      current_partitionAndSortKey,
      evaluationLimit: evaluationLimit01,
      nextPageHash: nextPageHash01,
      resultLimit: resultLimit01,
    });
    return result;
  }

  //@ts-ignore
  private async _mocody_getManyBySecondaryIndexPartiQlPaginateBase<
    TQuery,
    TData,
    TSortKeyField extends string | number,
  >({
    paramOption,
    canPaginate,
    enableRelationFetch,
  }: {
    paramOption: IMocodyQueryIndexOptions<TQuery, TSortKeyField>;
    canPaginate: boolean;
    enableRelationFetch: boolean;
  }): Promise<IMocodyPagingResult<TData[]>> {
    const {
      //
      tableFullName,
      secondaryIndexOptions,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw this._mocody_createGenericError("Invalid secondary index definitions");
    }

    const paramOption01 = { ...paramOption };

    if (!paramOption01?.indexName) {
      throw this._mocody_createGenericError("Invalid index name input");
    }

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === paramOption01.indexName;
    });

    if (!secondaryIndex) {
      throw this._mocody_createGenericError(
        `Secondary index '${paramOption01.indexName}' not named/defined in secondaryIndexOptions`,
      );
    }

    let projectionFields: (keyof TQuery)[] | undefined | null;

    const fields01 = this._mocody_getProjectionFields({
      fields: paramOption01.fields,
      excludeFields: paramOption01.excludeFields,
    });

    if (canPaginate && fields01?.length) {
      const fieldSet01 = new Set(fields01);
      fieldSet01.add(partitionKeyFieldName as any);
      projectionFields = Array.from(fieldSet01);
    } else {
      projectionFields = fields01;
    }

    let evaluationLimit01: number | undefined;
    let resultLimit01: number | undefined;

    if (paramOption01.limit && UtilService.isNumericInteger(paramOption01.limit)) {
      resultLimit01 = Number(paramOption01.limit);
    }

    if (paramOption01?.pagingParams?.evaluationLimit) {
      evaluationLimit01 = paramOption01?.pagingParams?.evaluationLimit;
    } else if (!paramOption01.query && resultLimit01) {
      evaluationLimit01 = resultLimit01;
    }

    const current_PartitionKeyFieldName = secondaryIndex.partitionKeyFieldName as string;
    const current_SortKeyFieldName = secondaryIndex.sortKeyFieldName as string;

    const default_partitionAndSortKey: [string, string] = [partitionKeyFieldName, sortKeyFieldName];
    const current_partitionAndSortKey: [string, string] = [current_PartitionKeyFieldName, current_SortKeyFieldName];

    const partitionSortKeyQuery = paramOption01.sortKeyQuery
      ? {
          ...{ [current_SortKeyFieldName]: paramOption01.sortKeyQuery },
          ...{ [current_PartitionKeyFieldName]: paramOption01.partitionKeyValue },
        }
      : { [current_PartitionKeyFieldName]: paramOption01.partitionKeyValue };

    if (!enableRelationFetch) {
      /** This block avoids query data leak */
      const localVariables = this._mocody_getLocalVariables();

      const hasFeatureEntity = [
        //
        current_PartitionKeyFieldName,
        current_SortKeyFieldName,
      ].includes(localVariables.sortKeyFieldName);

      if (!hasFeatureEntity) {
        paramOption01.query = (paramOption01.query || {}) as any;

        paramOption01.query = {
          ...paramOption01.query,
          ...this._mocody_featureEntity_Key_Value,
        } as any;
      } else if (current_PartitionKeyFieldName !== localVariables.sortKeyFieldName) {
        if (localVariables.sortKeyFieldName === current_SortKeyFieldName) {
          partitionSortKeyQuery[current_SortKeyFieldName] = { $eq: localVariables.featureEntityValue } as any;
        }
      }
    }

    const subStatement: string[] = [];
    const subParameter: any[] = [];

    const mainFilter = this._mocody_queryPartiQlFilter.processQueryFilter({
      queryDefs: partitionSortKeyQuery,
    });

    if (mainFilter?.subStatement) {
      subStatement.push(mainFilter.subStatement);
      subParameter.push(...mainFilter.subParameter);
    }

    if (paramOption01.query) {
      const otherFilter = this._mocody_queryPartiQlFilter.processQueryFilter({
        queryDefs: paramOption01.query,
      });
      subStatement.push(otherFilter.subStatement);
      subParameter.push(...otherFilter.subParameter);
    }

    const orderDesc = paramOption01?.sort === "desc";

    let nextPageHash01 = paramOption01?.pagingParams?.nextPageHash;

    if (nextPageHash01 === "undefined") {
      nextPageHash01 = undefined;
    }

    if (nextPageHash01 === "null") {
      nextPageHash01 = undefined;
    }

    const result = await this._mocody_queryPartiQlProcessor.mocody__helperDynamoQueryProcessor<TData>({
      dynamoDb: () => this._mocody_dynamoInit(),
      params: { subStatement, subParameter },
      orderDesc,
      canPaginate,
      tableFullName,
      indexName: paramOption01.indexName,
      featureEntityValue,
      projectionFields: projectionFields as string[],
      default_partitionAndSortKey,
      current_partitionAndSortKey,
      evaluationLimit: evaluationLimit01,
      nextPageHash: nextPageHash01,
      resultLimit: resultLimit01,
    });
    return result;
  }

  async mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T> {
    //
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ Del1SortKey: dataId });

    const {
      //
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._mocody_getLocalVariables();

    const dataExist = await this.mocody_getOneById({ dataId, withCondition });

    if (!(dataExist && dataExist[partitionKeyFieldName])) {
      throw this._mocody_errorHelper.mocody_helper_createFriendlyError("Record does NOT exists");
    }

    const params: DeleteItemCommandInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: { S: dataId },
        [sortKeyFieldName]: { S: featureEntityValue },
      },
    };

    try {
      await this._mocody_dynamoInit().deleteItem(params);
    } catch (err: any) {
      if (err && err.code === "ResourceNotFoundException") {
        throw this._mocody_errorHelper.mocody_helper_createFriendlyError("Table not found");
      } else if (err && err.code === "ResourceInUseException") {
        throw this._mocody_errorHelper.mocody_helper_createFriendlyError("Table in use");
      } else {
        throw err;
      }
    }
    return dataExist;
  }
}
