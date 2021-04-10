import type { IFuseFieldCondition, IFusePagingResult, IFuseQueryIndexOptions } from "../type/types";

export abstract class RepoModel<T> {
  protected abstract fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T | null>;

  protected abstract fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T[]>;

  protected abstract fuse_createOne({ data }: { data: T }): Promise<T>;

  protected abstract fuse_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T>;

  protected abstract fuse_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: Omit<IFuseQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<T[]>;

  protected abstract fuse_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>>;

  protected abstract fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T>;
}
