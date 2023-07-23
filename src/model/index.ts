import type {
  IMocodyFieldCondition,
  IMocodyPagingResult,
  IMocodyQueryIndexOptions,
  IMocodyPreparedTransaction,
  IMocodyTransactionPrepare,
} from "../type";

export abstract class RepoModel<T> {
  abstract mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T> | null;
  }): Promise<T | null>;

  abstract mocody_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined | null;
    withCondition?: IMocodyFieldCondition<T> | undefined | null;
  }): Promise<T[]>;

  abstract mocody_createOne({
    data,
    fieldAliases,
  }: {
    data: T;
    fieldAliases?: [keyof T, keyof T][] | undefined | null;
  }): Promise<T>;

  abstract mocody_prepareTransaction({
    transactPrepareInfo,
  }: {
    transactPrepareInfo: IMocodyTransactionPrepare<T>[];
  }): Promise<IMocodyPreparedTransaction[]>;

  abstract mocody_executeTransaction({ transactInfo }: { transactInfo: IMocodyPreparedTransaction[] }): Promise<void>;

  abstract mocody_formatForDump({ dataList }: { dataList: T[] }): Promise<string[]>;
  abstract mocody_validateFormatData({ data }: { data: T }): Promise<string>;

  abstract mocody_updateOne({
    dataId,
    updateData,
    withCondition,
    fieldAliases,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T> | null;
    fieldAliases?: [keyof T, keyof T][] | undefined | null;
  }): Promise<T>;

  abstract mocody_getManyByIndex<TData = T, TSortKeyField extends string | number = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<TData[]>;

  abstract mocody_getManyByIndexPaginate<TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>>;

  abstract mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T> | null;
  }): Promise<T>;

  abstract mocody_getManyWithRelation<TQuery = T, TData = T, TSortKeyField extends string | number = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TQuery, TSortKeyField>, "pagingParams">,
  ): Promise<TData[]>;

  abstract mocody_getManyWithRelationPaginate<TQuery = T, TData = T, TSortKeyField extends string | number = string>(
    paramOption: IMocodyQueryIndexOptions<TQuery, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>>;
}
