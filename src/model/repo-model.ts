import type { IMocodyFieldCondition, IMocodyPagingResult, IMocodyQueryIndexOptions } from "../type/types";

export abstract class RepoModel<T> {
  protected abstract mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T | null>;

  protected abstract mocody_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T[]>;

  protected abstract mocody_createOne({ data }: { data: T }): Promise<T>;

  protected abstract mocody_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;

  protected abstract mocody_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: Omit<IMocodyQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<T[]>;

  protected abstract mocody_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IMocodyQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IMocodyPagingResult<T[]>>;

  protected abstract mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IMocodyFieldCondition<T>;
  }): Promise<T>;
}
