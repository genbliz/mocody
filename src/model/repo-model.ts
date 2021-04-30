import type { IMocodyFieldCondition, IMocodyPagingResult, IMocodyQueryIndexOptions } from "../type/types";

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
  abstract mocody_formatDump({ dataList }: { dataList: T[] }): Promise<string>;

  abstract mocody_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;

  abstract mocody_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<T[]>;

  abstract mocody_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<T[]>>;

  abstract mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;
}
