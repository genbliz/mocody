import { SettingDefaults } from "./../helpers/constants";
import { UtilService } from "./../helpers/util-service";
import { LoggingService } from "./../helpers/logging-service";
import type {
  IMocodyFieldCondition,
  IMocodyIndexDefinition,
  IMocodyPagingResult,
  IMocodyQueryIndexOptions,
  IMocodyQueryIndexOptionsNoPaging,
} from "../type/types";
import { RepoModel } from "../model/repo-model";
import Joi from "joi";
import type { MocodyInitializerCouch } from "./couch-initializer";
import { coreSchemaDefinition, IMocodyCoreEntityModel } from "../core/base-schema";
import { MocodyErrorUtils, MocodyGenericError } from "../helpers/errors";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { CouchFilterQueryOperation } from "./couch-filter-query-operation";
import { CouchManageTable } from "./couch-manage-table";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  couchDb: () => MocodyInitializerCouch;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IMocodyCoreEntityModel;

type IFullEntity<T> = IMocodyCoreEntityModel & T;

export class CouchDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _mocody_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _mocody_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _mocody_operationNotSuccessful = "Operation Not Successful";
  private readonly _mocody_entityResultFieldKeysMap: Map<string, string>;
  private readonly _mocody_couchDb: () => MocodyInitializerCouch;
  private readonly _mocody_dataKeyGenerator: () => string;
  private readonly _mocody_schema: Joi.Schema;
  private readonly _mocody_tableFullName: string;
  private readonly _mocody_strictRequiredFields: string[];
  private readonly _mocody_featureEntityValue: string;
  private readonly _mocody_secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  private readonly _mocody_errorHelper: MocodyErrorUtils;
  private readonly _mocody_filterQueryOperation = new CouchFilterQueryOperation();
  //
  private _mocody_tableManager!: CouchManageTable<T>;

  constructor({
    schemaDef,
    couchDb,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._mocody_couchDb = couchDb;
    this._mocody_dataKeyGenerator = dataKeyGenerator;
    this._mocody_tableFullName = baseTableName;
    this._mocody_featureEntityValue = featureEntityValue;
    this._mocody_secondaryIndexOptions = secondaryIndexOptions;
    this._mocody_strictRequiredFields = strictRequiredFields as string[];
    this._mocody_errorHelper = new MocodyErrorUtils();
    this._mocody_entityResultFieldKeysMap = new Map();

    const fullSchemaMapDef = {
      ...schemaDef,
      ...coreSchemaDefinition,
    };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._mocody_entityResultFieldKeysMap.set(key, key);
    });

    this._mocody_schema = Joi.object().keys({
      ...fullSchemaMapDef,
      _id: Joi.string().required().min(5).max(512),
    });
  }

  mocody_tableManager() {
    if (!this._mocody_tableManager) {
      this._mocody_tableManager = new CouchManageTable<T>({
        couchDb: () => this._mocody_couchDb(),
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

  private _mocody_couchDbInstance() {
    return this._mocody_couchDb().getDocInstance();
  }

  private _mocody_getLocalVariables() {
    return {
      partitionKeyFieldName: this._mocody_partitionKeyFieldName,
      sortKeyFieldName: this._mocody_sortKeyFieldName,
      //
      featureEntityValue: this._mocody_featureEntityValue,
      //
      // tableFullName: this._mocody_tableFullName,
      secondaryIndexOptions: this._mocody_secondaryIndexOptions,
      strictRequiredFields: this._mocody_strictRequiredFields,
    } as const;
  }

  private _mocody_stripNonRequiredOutputData({
    dataObj,
    excludeFields,
  }: {
    dataObj: Record<string, any>;
    excludeFields?: string[];
  }): T {
    const returnData = {} as any;
    if (typeof dataObj === "object" && this._mocody_entityResultFieldKeysMap.size > 0) {
      Object.entries({ ...dataObj }).forEach(([key, value]) => {
        if (this._mocody_entityResultFieldKeysMap.has(key)) {
          if (excludeFields?.length) {
            if (!excludeFields.includes(key)) {
              returnData[key] = value;
            }
          } else {
            returnData[key] = value;
          }
        }
      });
    }
    return returnData;
  }

  private _mocody_getNativePouchId(dataId: string) {
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
    item: any;
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
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._mocody_createGenericError(`Strict required field: '${field}', NOT defined`);
        }
      }
    }
  }

  private _mocody_removeDuplicateString(list: string[]) {
    return Array.from(new Set(list));
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

  async mocody_createOne({ data }: { data: T }): Promise<T> {
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
      _id: this._mocody_getNativePouchId(dataId),
    };

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._mocody_createGenericError("FeatureEntity mismatched");
    }

    const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);
    this._mocody_checkValidateStrictRequiredFields(validatedData);

    const couch = await this._mocody_couchDbInstance();

    const result = await couch.insert(validatedData);
    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({
      dataObj: validatedData,
    });
  }

  async mocody_getAll({ size, skip }: { size?: number; skip?: number } = {}): Promise<T[]> {
    const couch = await this._mocody_couchDbInstance();
    const data = await couch.list({
      include_docs: true,
      startkey: this._mocody_featureEntityValue,
      endkey: `${this._mocody_featureEntityValue}\ufff0`,
      limit: size,
      skip,
    });
    const dataList: T[] = [];
    data?.rows?.forEach((item) => {
      if (item?.doc?.featureEntity === this._mocody_featureEntityValue) {
        const doc = this._mocody_stripNonRequiredOutputData({ dataObj: item.doc });
        dataList.push(doc);
      }
    });
    return dataList;
  }

  async mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T | null> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const nativeId = this._mocody_getNativePouchId(dataId);

    const couch = await this._mocody_couchDbInstance();
    const dataInDb = await couch.get(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      return null;
    }
    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return this._mocody_stripNonRequiredOutputData({ dataObj: dataInDb });
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

    const nativeId = this._mocody_getNativePouchId(dataId);

    const couch = await this._mocody_couchDbInstance();
    const dataInDb = await couch.get(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue && dataInDb._rev)) {
      throw this._mocody_createGenericError("Record does not exists");
    }
    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._mocody_createGenericError("Update conditions NOT passed");
    }

    const dataMust = this._mocody_getBaseObject({ dataId });

    const dataInDbPlain = UtilService.convertObjectPlainObject(dataInDb);
    const neededData = this._mocody_stripNonRequiredOutputData({ dataObj: dataInDbPlain });

    const data: IFullEntity<T> = {
      ...neededData,
      ...updateData,
      ...dataMust,
      _id: dataInDb._id,
    };

    const { validatedData } = await this._mocody_allHelpValidateGetValue(data);

    this._mocody_checkValidateStrictRequiredFields(validatedData);

    const result = await couch.insert({
      ...validatedData,
      _rev: dataInDb._rev,
    });

    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({
      dataObj: validatedData,
    });
  }

  async mocody_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IMocodyFieldCondition<T> | undefined;
  }): Promise<T[]> {
    //
    const uniqueIds = this._mocody_removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._mocody_getNativePouchId(id));

    const couch = await this._mocody_couchDbInstance();
    const data = await couch.list({
      keys: fullUniqueIds,
      include_docs: true,
    });

    const dataList: T[] = [];

    if (withCondition?.length) {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._mocody_featureEntityValue) {
          const passed = this._mocody_withConditionPassed({ item: item.doc, withCondition });
          if (passed) {
            const k = this._mocody_stripNonRequiredOutputData({
              dataObj: item.doc,
              excludeFields: fields as any[],
            });
            dataList.push(k);
          }
        }
      });
    } else {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._mocody_featureEntityValue) {
          const k = this._mocody_stripNonRequiredOutputData({
            dataObj: item.doc,
            excludeFields: fields as any[],
          });
          dataList.push(k);
        }
      });
    }
    return dataList;
  }

  async mocody_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptionsNoPaging<TData, TSortKeyField>,
  ): Promise<T[]> {
    const result = await this._mocody_getManyBySecondaryIndexPaginateBase<TData, TSortKeyField>({
      paramOption,
      canPaginate: false,
    });
    if (result?.mainResult?.length) {
      return result.mainResult;
    }
    return [];
  }

  async mocody_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
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

    paramOption.query = (paramOption.query || {}) as any;

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

    const queryDefs = {
      ...paramOption.query,
      ...partitionSortKeyQuery,
    };

    const queryDefData = this._mocody_filterQueryOperation.processQueryFilter({ queryDefs });

    const query01: Record<string, any> = {};
    const query02: Record<string, any> = {};
    const query03: Record<string, any> = {};

    Object.entries(queryDefData).forEach(([key, val]) => {
      if (key === index_PartitionKeyFieldName) {
        query01[key] = val;
      } else if (key === index_SortKeyFieldName) {
        query02[key] = val;
      } else {
        query03[key] = val;
      }
    });

    const queryDefDataOrdered = { ...query01, ...query02, ...query03 };
    const sort01: Array<{ [propName: string]: "asc" | "desc" }> = [];

    if (paramOption.sort === "desc") {
      sort01.push({ [index_PartitionKeyFieldName]: "desc" });
      sort01.push({ [index_SortKeyFieldName]: "desc" });
    } else {
      sort01.push({ [index_PartitionKeyFieldName]: "asc" });
      sort01.push({ [index_SortKeyFieldName]: "asc" });
    }

    LoggingService.log({
      queryDefDataOrdered,
      sort: sort01,
      paramOption,
    });

    let projection: string[] | undefined = undefined;

    if (paramOption?.fields?.length) {
      projection = this._mocody_removeDuplicateString(paramOption.fields as any);
    }

    let nextPageHash: string | undefined = undefined;

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
          if (param01.limit) {
            pagingOptions.limit = param01.limit;
          }
          if (param01.pageNo) {
            pagingOptions.pageNo = param01.pageNo;
          }
        }
      } catch (error) {
        LoggingService.log(error?.message);
      }
      moreFindOption.limit = pagingOptions.limit;
      //
      const skipValue = pagingOptions.limit * (pagingOptions.pageNo || 0);
      //
      if (skipValue) {
        moreFindOption.skip = skipValue;
      }
    } else {
      if (paramOption.limit && UtilService.isNumericInteger(paramOption.limit)) {
        moreFindOption.limit = Number(paramOption.limit);
      }
    }

    const couch = await this._mocody_couchDbInstance();
    const data = await couch.partitionedFind(this._mocody_featureEntityValue, {
      selector: { ...queryDefDataOrdered },
      fields: projection,
      use_index: paramOption.indexName,
      sort: sort01?.length ? sort01 : undefined,
      limit: moreFindOption.limit,
      skip: moreFindOption.skip,
      // bookmark: paramOption?.pagingParams?.nextPageHash,
    });

    const results = data?.docs?.map((item) => {
      return this._mocody_stripNonRequiredOutputData({ dataObj: item });
    });

    if (canPaginate && results.length && moreFindOption.limit && results.length >= moreFindOption.limit) {
      pagingOptions.pageNo = pagingOptions.pageNo + 1;
      nextPageHash = UtilService.encodeStringToBase64(JSON.stringify(pagingOptions));
    }

    return {
      mainResult: results,
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
    const nativeId = this._mocody_getNativePouchId(dataId);
    const couch = await this._mocody_couchDbInstance();
    const dataInDb = await couch.get(nativeId);

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      throw this._mocody_createGenericError("Record does not exists");
    }
    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._mocody_createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await couch.destroy(dataInDb._id, dataInDb._rev);
    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({ dataObj: dataInDb });
  }
}
