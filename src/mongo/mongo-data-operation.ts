import { MocodyUtil } from "./../helpers/mocody-utils";
import { SettingDefaults } from "./../helpers/constants";
import { UtilService } from "./../helpers/util-service";
import { LoggingService } from "./../helpers/logging-service";
import {
  IMocodyFieldCondition,
  IMocodyIndexDefinition,
  IMocodyPagingResult,
  IMocodyPreparedTransaction,
  IMocodyQueryIndexOptions,
  IMocodyQueryIndexOptionsNoPaging,
  IMocodyTransactionPrepare,
} from "../type";
import { RepoModel } from "../model";
import Joi from "joi";
import { coreSchemaDefinition, IMocodyCoreEntityModel } from "../core/base-schema";
import { MocodyErrorUtils, MocodyGenericError } from "../helpers/errors";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { MocodyInitializerMongo } from "./mongo-initializer";
import { MongoFilterQueryOperation } from "./mongo-filter-query-operation";
import { MongoManageTable } from "./mongo-table-manager";
import { ReadConcern, ReadPreference, SortDirection, TransactionOptions, WriteConcern } from "mongodb";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  mongoDbInitializer: () => MocodyInitializerMongo;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IMocodyCoreEntityModel;

type IFullEntity<T> = IMocodyCoreEntityModel & T;
type IFullEntityNative<T> = IMocodyCoreEntityModel & { _id: string } & T;

export class MongoDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _mocody_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _mocody_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _mocody_operationNotSuccessful = "Operation Not Successful";
  private readonly _mocody_entityFieldsKeySet: Set<keyof T>;
  private readonly _mocody_mongoDb: () => MocodyInitializerMongo;
  private readonly _mocody_dataKeyGenerator: () => string;
  private readonly _mocody_schema: Joi.Schema;
  private readonly _mocody_tableFullName: string;
  private readonly _mocody_strictRequiredFields: string[];
  private readonly _mocody_featureEntityValue: string;
  private readonly _mocody_secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  private readonly _mocody_errorHelper: MocodyErrorUtils;
  private readonly _mocody_filterQueryOperation = new MongoFilterQueryOperation();
  //
  private _mocody_tableManager!: MongoManageTable<T>;

  constructor({
    schemaDef,
    mongoDbInitializer,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._mocody_mongoDb = mongoDbInitializer;
    this._mocody_dataKeyGenerator = dataKeyGenerator;
    this._mocody_tableFullName = baseTableName;
    this._mocody_featureEntityValue = featureEntityValue;
    this._mocody_secondaryIndexOptions = secondaryIndexOptions;
    this._mocody_strictRequiredFields = strictRequiredFields as string[];
    this._mocody_errorHelper = new MocodyErrorUtils();
    this._mocody_entityFieldsKeySet = new Set();

    const fullSchemaMapDef = {
      ...schemaDef,
      ...coreSchemaDefinition,
    };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._mocody_entityFieldsKeySet.add(key as keyof T);
    });

    this._mocody_schema = Joi.object().keys({
      ...fullSchemaMapDef,
      _id: Joi.string().required().min(5).max(512),
    });
  }

  mocody_tableManager() {
    if (!this._mocody_tableManager) {
      this._mocody_tableManager = new MongoManageTable<T>({
        mongoDb: () => this._mocody_mongoDb(),
        secondaryIndexOptions: this._mocody_secondaryIndexOptions,
        tableFullName: this._mocody_tableFullName,
        partitionKeyFieldName: this._mocody_partitionKeyFieldName,
        sortKeyFieldName: this._mocody_sortKeyFieldName,
      });
    }
    return this._mocody_tableManager;
  }

  private _mocody_generateDynamoTableKey() {
    return this._mocody_dataKeyGenerator();
  }

  private async _mocody_getDbInstance() {
    return await this._mocody_mongoDb()
      //
      .getCustomCollectionInstance<IFullEntity<T>>(this._mocody_tableFullName);
  }

  private async _mocody_getDbInstanceDefined(tableFullName: string) {
    return await this._mocody_mongoDb()
      //
      .getCustomCollectionInstance<IFullEntity<T>>(tableFullName);
  }

  private _mocody_getLocalVariables() {
    return {
      partitionKeyFieldName: this._mocody_partitionKeyFieldName,
      sortKeyFieldName: this._mocody_sortKeyFieldName,
      //
      featureEntityValue: this._mocody_featureEntityValue,
      //
      secondaryIndexOptions: this._mocody_secondaryIndexOptions,
      strictRequiredFields: this._mocody_strictRequiredFields,
    } as const;
  }

  private _mocody_getNativeMongoId(dataId: string) {
    const { featureEntityValue } = this._mocody_getLocalVariables();
    return [featureEntityValue, dataId].join(":");
  }

  private _mocody_getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._mocody_getLocalVariables();

    const dataMust = {
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _mocody_withConditionPassed({
    item,
    withCondition,
  }: {
    item: Record<keyof T, any>;
    withCondition?: IMocodyFieldCondition<T>;
  }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
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

  private _mocody_removeDuplicateString(list: string[]) {
    return Array.from(new Set(list));
  }

  private _mocody_getProjectionFields<TProj = T>({
    excludeFields,
    fields,
  }: {
    excludeFields?: (keyof TProj)[];
    fields?: (keyof TProj)[];
  }) {
    return MocodyUtil.getProjectionFields({
      excludeFields,
      fields,
      entityFields: Array.from(this._mocody_entityFieldsKeySet) as any[],
    });
  }

  private _mocody_toMongoProjection(fields?: (keyof T)[]) {
    if (fields?.length) {
      const projection: Record<string, any> = {};
      const uniqueFields = this._mocody_removeDuplicateString(fields as string[]);
      uniqueFields.forEach((field) => {
        projection[field] = 1;
      });
      return projection;
    }
    return undefined;
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

  private _mocody_createGenericError(error: string) {
    return new MocodyGenericError(error);
  }

  async mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T | null> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const mongo = await this._mocody_getDbInstance();

    const nativeId = this._mocody_getNativeMongoId(dataId);
    const query: any = { _id: nativeId };
    const dataInDb = await mongo.findOne(query, { projection: { _id: 0 } });

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      return null;
    }
    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return dataInDb;
  }

  async mocody_getManyByIds({
    dataIds,
    fields,
    excludeFields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined;
    excludeFields?: (keyof T)[] | undefined;
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T[]> {
    const uniqueIds = this._mocody_removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._mocody_getNativeMongoId(id));

    const mongo = await this._mocody_getDbInstance();

    const fieldKeys = this._mocody_getProjectionFields({ fields, excludeFields });

    if (withCondition?.length && fieldKeys?.length) {
      withCondition.forEach((item) => {
        fieldKeys.push(item.field);
      });
    }

    const projection = this._mocody_toMongoProjection(fieldKeys) ?? ({ _id: -1 } as any);

    const query: any = { _id: { $in: fullUniqueIds } };

    const dataListInDb = await mongo.find<IFullEntityNative<T>>(query, { projection }).toArray();

    if (!dataListInDb?.length) {
      return [];
    }

    if (withCondition?.length) {
      const dataFiltered = dataListInDb.filter((item) => {
        const passed = this._mocody_withConditionPassed({ item, withCondition });
        return passed;
      });
      return dataFiltered;
    }

    return dataListInDb;
  }

  async mocody_formatDump({ dataList }: { dataList: T[] }): Promise<string> {
    const bulkData: string[] = [];

    for (const data of dataList) {
      const { validatedDataWithTTL } = await this._mocody_validateReady({ data });
      bulkData.push(JSON.stringify(validatedDataWithTTL));
    }
    return bulkData.join("\n");
  }

  async mocody_createOne({ data }: { data: T }): Promise<T> {
    const { validatedData, validatedDataWithTTL } = await this._mocody_validateReady({ data });

    const mongo = await this._mocody_getDbInstance();
    const result = await mongo.insertOne(validatedDataWithTTL);

    if (!result?.acknowledged) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    const final = { ...validatedData };
    delete final._id;
    return final;
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
    const fullData = {
      ...data,
      ...dataMust,
      _id: this._mocody_getNativeMongoId(dataId),
    };

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._mocody_createGenericError("FeatureEntity mismatched");
    }

    const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);
    this._mocody_checkValidateStrictRequiredFields(validatedData);

    const validatedDataWithTTL: any = this._mocody_formatTTL(validatedData);

    return { validatedData, validatedDataWithTTL };
  }

  private _mocody_formatTTL(fullData: IFullEntity<T>) {
    if (fullData?.dangerouslyExpireAt) {
      fullData.dangerouslyExpireAtTTL = new Date(fullData.dangerouslyExpireAt);
    } else {
      delete fullData.dangerouslyExpireAtTTL;
    }
    return fullData;
  }

  async mocody_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const nativeId = this._mocody_getNativeMongoId(dataId);
    const query = { _id: nativeId } as IFullEntityNative<T>;

    const mongo = await this._mocody_getDbInstance();

    const dataInDb = await mongo.findOne(query);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      throw this._mocody_createGenericError("Record does not exists");
    }

    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._mocody_createGenericError("Record with conditions does not exists");
    }

    const dataMust = this._mocody_getBaseObject({ dataId });

    const data: IFullEntity<T> = {
      ...dataInDb,
      ...updateData,
      ...dataMust,
    };

    const { validatedData } = await this._mocody_allHelpValidateGetValue(data);

    const result = await mongo.replaceOne(query, validatedData);
    if (!result.modifiedCount) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    const final = { ...validatedData };
    delete final._id;
    return final;
  }

  async mocody_prepareTransaction({
    transactPrepareInfo,
  }: {
    transactPrepareInfo: IMocodyTransactionPrepare<T>[];
  }): Promise<IMocodyPreparedTransaction[]> {
    const transactData: IMocodyPreparedTransaction[] = [];

    const { partitionKeyFieldName } = this._mocody_getLocalVariables();

    for (const item of transactPrepareInfo) {
      if (item.kind === "create") {
        const { validatedDataWithTTL } = await this._mocody_validateReady({ data: item.data });
        transactData.push({
          data: validatedDataWithTTL,
          partitionKeyFieldName,
          tableName: this._mocody_tableFullName,
          kind: item.kind,
        });
      } else if (item.kind === "update") {
        const { validatedDataWithTTL } = await this._mocody_validateReady({ data: item.data });
        const nativeId = this._mocody_getNativeMongoId(item.dataId);
        transactData.push({
          data: validatedDataWithTTL,
          partitionKeyFieldName,
          tableName: this._mocody_tableFullName,
          kind: item.kind,
          keyQuery: { _id: nativeId },
        });
      } else if (item.kind === "delete") {
        const nativeId = this._mocody_getNativeMongoId(item.dataId);
        transactData.push({
          partitionKeyFieldName,
          tableName: this._mocody_tableFullName,
          kind: item.kind,
          keyQuery: { _id: nativeId },
        });
      }
    }
    return transactData;
  }

  async mocody_executeTransaction({ transactInfo }: { transactInfo: IMocodyPreparedTransaction[] }): Promise<void> {
    const session = await this._mocody_mongoDb().getNewSession();
    try {
      const transactionOptions: TransactionOptions = {
        readPreference: new ReadPreference("primary"),
        readConcern: new ReadConcern("local"),
        writeConcern: new WriteConcern("majority"),
      };
      await session.withTransaction(async () => {
        for (const item of transactInfo) {
          if (item.kind === "create") {
            const mongo = await this._mocody_getDbInstanceDefined(item.tableName);
            await mongo.insertOne(item.data as any, { session });
            //
          } else if (item.kind === "update") {
            const mongo = await this._mocody_getDbInstanceDefined(item.tableName);
            const { validatedData } = await this._mocody_allHelpValidateGetValue(item.data);
            await mongo.replaceOne(item.keyQuery as any, validatedData);
            //
          } else if (item.kind === "delete") {
            const mongo = await this._mocody_getDbInstanceDefined(item.tableName);
            await mongo.deleteOne(item.keyQuery as any);
          }
        }
      }, transactionOptions);
      await session.commitTransaction();
      await session.endSession();
    } catch (e) {
      await session.abortTransaction();
      await session.endSession();
      throw e;
    }
  }

  async mocody_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptionsNoPaging<TData, TSortKeyField>,
  ): Promise<T[]> {
    const result = await this._mocody_getManyBySecondaryIndexPaginateBase<TData, TSortKeyField>({
      paramOption,
      canPaginate: false,
    });
    if (result?.paginationResults) {
      return result.paginationResults;
    }
    return [];
  }

  mocody_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<T[]>> {
    return this._mocody_getManyBySecondaryIndexPaginateBase<TData, TSortKeyField>({
      paramOption,
      canPaginate: true,
    });
  }

  private async _mocody_getManyBySecondaryIndexPaginateBase<TData = T, TSortKeyField = string>({
    paramOption,
    canPaginate,
  }: {
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>;
    canPaginate: boolean;
  }): Promise<IMocodyPagingResult<T[]>> {
    const { secondaryIndexOptions } = this._mocody_getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw this._mocody_createGenericError("Invalid secondary index definitions");
    }

    if (!paramOption?.indexName) {
      throw this._mocody_createGenericError("Invalid index name input");
    }

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === paramOption.indexName;
    });

    if (!secondaryIndex) {
      throw this._mocody_createGenericError("Secondary index not named/defined");
    }

    const index_PartitionKeyFieldName = secondaryIndex.partitionKeyFieldName as string;
    const index_SortKeyFieldName = secondaryIndex.sortKeyFieldName as string;

    const partitionSortKeyQuery = paramOption.sortKeyQuery
      ? {
          ...{ [index_SortKeyFieldName]: paramOption.sortKeyQuery },
          ...{ [index_PartitionKeyFieldName]: paramOption.partitionKeyValue },
        }
      : { [index_PartitionKeyFieldName]: paramOption.partitionKeyValue };

    const localVariables = this._mocody_getLocalVariables();

    /** Avoid query data leak */
    const hasFeatureEntity = [
      //
      index_PartitionKeyFieldName,
      index_SortKeyFieldName,
    ].includes(localVariables.sortKeyFieldName);

    paramOption.query = paramOption.query || ({} as any);

    if (!hasFeatureEntity) {
      paramOption.query = {
        ...paramOption.query,
        ...{ [localVariables.sortKeyFieldName]: localVariables.featureEntityValue },
      } as any;
    } else if (index_PartitionKeyFieldName !== localVariables.sortKeyFieldName) {
      if (localVariables.sortKeyFieldName === index_SortKeyFieldName) {
        partitionSortKeyQuery[index_SortKeyFieldName] = { $eq: localVariables.featureEntityValue as any };
      }
    }

    const queryDefs: any = {
      ...paramOption.query,
      ...partitionSortKeyQuery,
    };

    const queryDefData: any = this._mocody_filterQueryOperation.processQueryFilter({ queryDefs });

    const fieldKeys = this._mocody_getProjectionFields({
      fields: paramOption.fields,
      excludeFields: paramOption.excludeFields,
    });

    const projection = this._mocody_toMongoProjection(fieldKeys as any[]) ?? { _id: 0 };

    const sort01: [string, SortDirection][] = [];

    if (paramOption.sort === "desc") {
      sort01.push([index_PartitionKeyFieldName, -1]);
      sort01.push([index_SortKeyFieldName, -1]);
    } else {
      sort01.push([index_PartitionKeyFieldName, 1]);
      sort01.push([index_SortKeyFieldName, 1]);
    }

    let nextPageHash: string | undefined;

    type IPaging = {
      pageNo: number;
      limit: number;
    };

    type IMoreFindOption = {
      skip: number | undefined;
      limit: number | undefined;
    };

    const pagingOptions: IPaging = {
      pageNo: 0,
      limit: SettingDefaults.PAGE_SIZE,
    };

    const moreFindOption: IMoreFindOption = {
      limit: undefined,
      skip: undefined,
    };

    if (canPaginate) {
      if (paramOption.limit && UtilService.isNumericInteger(paramOption.limit)) {
        pagingOptions.limit = Number(paramOption.limit);
      }

      try {
        const nextPageHash01 = paramOption?.pagingParams?.nextPageHash;
        if (nextPageHash01) {
          const param01: IPaging = JSON.parse(UtilService.decodeStringFromBase64(nextPageHash01));
          if (UtilService.isNumericInteger(param01.limit)) {
            pagingOptions.limit = Number(param01.limit);
          }
          if (UtilService.isNumericInteger(param01.pageNo)) {
            pagingOptions.pageNo = Number(param01.pageNo);
          }
        }
      } catch (error: any) {
        LoggingService.log(error?.message);
      }

      const skipValue = pagingOptions.limit * (pagingOptions.pageNo || 0);
      //
      if (skipValue) {
        moreFindOption.skip = skipValue;
      }
      moreFindOption.limit = pagingOptions.limit;
      //
    } else {
      if (paramOption.limit && UtilService.isNumericInteger(paramOption.limit)) {
        moreFindOption.limit = Number(paramOption.limit);
      }
    }

    const mongo = await this._mocody_getDbInstance();

    const results = await mongo
      .find<T>(queryDefData, {
        projection: projection,
        sort: sort01.length ? sort01 : undefined,
        limit: moreFindOption.limit,
        skip: moreFindOption.skip,
      })
      .hint(paramOption.indexName)
      .toArray();

    if (canPaginate && results.length && results.length >= pagingOptions.limit) {
      pagingOptions.pageNo = pagingOptions.pageNo + 1;
      nextPageHash = UtilService.encodeStringToBase64(JSON.stringify(pagingOptions));
    }

    return {
      paginationResults: results,
      nextPageHash: nextPageHash,
    };
  }

  async mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const db = await this._mocody_getDbInstance();

    const nativeId = this._mocody_getNativeMongoId(dataId);
    const query = { _id: nativeId } as IFullEntityNative<T>;
    const dataInDb = await db.findOne(query);

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      throw this._mocody_createGenericError("Record does not exists");
    }
    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._mocody_createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await db.deleteOne(query);
    if (!result?.deletedCount) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return dataInDb;
  }
}
