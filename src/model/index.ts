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
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T | null>;

  abstract mocody_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T[]>;

  abstract mocody_createOne({ data }: { data: T }): Promise<T>;

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
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;

  abstract mocody_getManyByIndex<TData = T, TSortKeyField = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<TData[]>;

  abstract mocody_getManyByIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>>;

  abstract mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;

  abstract mocody_getManyWithRelation<TQuery = T, TData = T, TSortKeyField = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TQuery, TSortKeyField>, "pagingParams">,
  ): Promise<TData[]>;

  abstract mocody_getManyWithRelationPaginate<TQuery = T, TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptions<TQuery, TSortKeyField>,
  ): Promise<IMocodyPagingResult<TData[]>>;
}
