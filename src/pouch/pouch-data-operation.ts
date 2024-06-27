import { MocodyUtil } from "../helpers/mocody-utils";
import { SettingDefaults } from "../helpers/constants";
import { UtilService } from "../helpers/util-service";
import { LoggingService } from "../helpers/logging-service";
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
import type { MocodyInitializerPouch } from "./pouch-initializer";
import { coreSchemaDefinition, IMocodyCoreEntityModel } from "../core/base-schema";
import { MocodyErrorUtils, MocodyGenericError } from "../helpers/errors";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { PouchFilterQueryOperation } from "./pouch-filter-query-operation";
import { PouchManageTable } from "./pouch-manage-table";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  pouchDbInitializer: () => MocodyInitializerPouch;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IMocodyCoreEntityModel;

type IFullEntity<T> = IMocodyCoreEntityModel & T;

export class PouchDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _mocody_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _mocody_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _mocody_operationNotSuccessful = "Operation Not Successful";
  private readonly _mocody_entityFieldsKeySet: Set<keyof T>;
  private readonly _mocody_pouchDb: () => MocodyInitializerPouch;
  private readonly _mocody_dataKeyGenerator: () => string;
  private readonly _mocody_schema: Joi.Schema;
  private readonly _mocody_tableFullName: string;
  private readonly _mocody_strictRequiredFields: string[];
  private readonly _mocody_featureEntityValue: string;
  private readonly _mocody_secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  private readonly _mocody_errorHelper: MocodyErrorUtils;
  private readonly _mocody_filterQueryOperation = new PouchFilterQueryOperation();
  //
  private _mocody_tableManager!: PouchManageTable<T>;

  constructor({
    schemaDef,
    pouchDbInitializer,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._mocody_pouchDb = pouchDbInitializer;
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
      this._mocody_tableManager = new PouchManageTable<T>({
        pouchDb: () => this._mocody_pouchDb(),
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

  private _mocody_pouchDbInstance() {
    return this._mocody_pouchDb();
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

  private _mocody_stripNonRequiredOutputData<TData = T>({
    dataObj,
    excludeFields,
  }: {
    dataObj: Record<string, any>;
    excludeFields?: string[] | undefined | null;
  }): TData {
    const returnData = {} as any;
    if (dataObj && typeof dataObj === "object" && this._mocody_entityFieldsKeySet.size > 0) {
      Object.entries({ ...dataObj }).forEach(([key, value]) => {
        if (this._mocody_entityFieldsKeySet.has(key as keyof T)) {
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
    const { validatedData } = await this._mocody_validateReady({ data });

    const result = await this._mocody_pouchDbInstance().createDoc({
      validatedData,
    });
    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({
      dataObj: validatedData,
    });
  }

  async mocody_formatForDump({ dataList }: { dataList: T[] }): Promise<string[]> {
    const bulkData: string[] = [];

    for (const data of dataList) {
      const validatedData = await this.mocody_validateFormatData({ data });
      bulkData.push(validatedData);
    }
    return bulkData;
  }

  async mocody_validateFormatData({ data }: { data: T }): Promise<string> {
    const { validatedData } = await this._mocody_validateReady({ data });
    return JSON.stringify(validatedData);
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
      _id: this._mocody_getNativePouchId(dataId),
    };

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._mocody_createGenericError("FeatureEntity mismatched");
    }

    const { validatedData } = await this._mocody_allHelpValidateGetValue(fullData);
    this._mocody_checkValidateStrictRequiredFields(validatedData);

    return { validatedData };
  }

  async mocody_getAll({ size, skip }: { size?: number | undefined | null; skip?: number | undefined | null }): Promise<T[]> {
    const data = await this._mocody_pouchDbInstance().getList({
      featureEntity: this._mocody_featureEntityValue,
      size,
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
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }): Promise<T | null> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const nativeId = this._mocody_getNativePouchId(dataId);

    const dataInDb = await this._mocody_pouchDbInstance().getById({ nativeId });

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
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }): Promise<T> {
    this._mocody_errorHelper.mocody_helper_validateRequiredString({ dataId });

    const nativeId = this._mocody_getNativePouchId(dataId);

    const dataInDb = await this._mocody_pouchDbInstance().getById({
      nativeId,
    });

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

    const result = await this._mocody_pouchDbInstance().updateDoc({
      validatedData,
      docRev: dataInDb._rev,
    });

    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({
      dataObj: validatedData,
    });
  }

  /** Not implemented */
  async mocody_prepareTransaction({
    transactPrepareInfo,
  }: {
    transactPrepareInfo: IMocodyTransactionPrepare<T>[];
  }): Promise<IMocodyPreparedTransaction[]> {
    await Promise.resolve();
    throw new Error("Pouch:PrepareTransaction not implemented.");
  }

  /** Not implemented */
  async mocody_executeTransaction({ transactInfo }: { transactInfo: IMocodyPreparedTransaction[] }): Promise<void> {
    await Promise.resolve();
    throw new Error("Pouch:ExecuteTransaction not implemented.");
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
  }): Promise<T[]> {
    const dataList: T[] = [];

    if (!dataIds?.length) {
      return dataList;
    }

    dataIds.forEach((dataId) => {
      this._mocody_errorHelper.mocody_helper_validateRequiredString({ BatchGetDataId: dataId });
    });

    const uniqueIds = this._mocody_removeDuplicateString(dataIds);

    if (uniqueIds?.length === 1) {
      const result = await this.mocody_getOneById({
        dataId: uniqueIds[0],
        withCondition,
      });

      if (result) {
        if (fields?.length) {
          const result01 = UtilService.pickFromObject({ dataObject: result, pickKeys: fields });
          dataList.push(result01);
        } else {
          dataList.push(result);
        }
      }
      return dataList;
    }

    const nativeIds = uniqueIds.map((id) => this._mocody_getNativePouchId(id));

    const data = await this._mocody_pouchDbInstance().getManyByIds({
      nativeIds,
    });

    const fieldKeys = this._mocody_getProjectionFields({ fields, excludeFields });

    if (withCondition?.length) {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._mocody_featureEntityValue) {
          const passed = this._mocody_withConditionPassed({ item: item.doc, withCondition });
          if (passed) {
            const k = this._mocody_stripNonRequiredOutputData({
              dataObj: item.doc,
              excludeFields: fieldKeys as any[],
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
            excludeFields: fieldKeys as any[],
          });
          dataList.push(k);
        }
      });
    }
    return dataList;
  }

  async mocody_getManyByIndex<TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptionsNoPaging<TData, TSortKeyField>,
  ): Promise<TData[]> {
    const result = await this._mocody_getManyBySecondaryIndexPaginateBase<TData, TData, TSortKeyField>({
      paramOption,
      canPaginate: false,
      enableRelationFetch: false,
    });
    if (result?.paginationResults?.length) {
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
    paramOption: Omit<IMocodyQueryIndexOptions<TQuery, TSortKeyField>, "pagingParams"> & {},
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

    if (!enableRelationFetch) {
      /** This block avoids query data leak */
      const localVariables = this._mocody_getLocalVariables();

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
          partitionSortKeyQuery[index_SortKeyFieldName] = { $eq: localVariables.featureEntityValue } as any;
        }
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

    let projection: string[] | undefined;

    const fieldKeys = this._mocody_getProjectionFields({
      fields: paramOption.fields,
      excludeFields: paramOption.excludeFields,
    });

    if (fieldKeys?.length) {
      projection = this._mocody_removeDuplicateString(fieldKeys as any[]);
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
          if (param01.limit) {
            pagingOptions.limit = param01.limit;
          }
          if (param01.pageNo) {
            pagingOptions.pageNo = param01.pageNo;
          }
        }
      } catch (error: any) {
        LoggingService.log(error);
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

    const data = await this._mocody_pouchDbInstance().findPartitionedDocs({
      featureEntity: this._mocody_featureEntityValue,
      selector: { ...queryDefDataOrdered },
      fields: projection,
      use_index: paramOption.indexName,
      sort: sort01?.length ? sort01 : undefined,
      limit: moreFindOption.limit,
      skip: moreFindOption.skip,
    });

    const results = data?.docs?.map((item) => {
      return this._mocody_stripNonRequiredOutputData<TData>({ dataObj: item });
    });

    if (canPaginate && results.length && moreFindOption.limit && results.length >= moreFindOption.limit) {
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
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }): Promise<T> {
    const nativeId = this._mocody_getNativePouchId(dataId);

    const dataInDb = await this._mocody_pouchDbInstance().getById({
      nativeId,
    });

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._mocody_featureEntityValue)) {
      throw this._mocody_createGenericError("Record does not exists");
    }

    const passed = this._mocody_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._mocody_createGenericError("Record with conditions does not exists for deletion");
    }

    const result = await this._mocody_pouchDbInstance().deleteById({
      nativeId: dataInDb._id,
      docRev: dataInDb._rev,
    });

    if (!result.ok) {
      throw this._mocody_createGenericError(this._mocody_operationNotSuccessful);
    }
    return this._mocody_stripNonRequiredOutputData({ dataObj: dataInDb });
  }
}
