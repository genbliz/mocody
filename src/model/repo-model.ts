import type { IFuseFieldCondition, IFusePagingResult, IFuseQueryIndexOptions } from "../type/types";

export abstract class RepoModel<T> {
  protected abstract mocody_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T | null>;

  protected abstract mocody_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T[]>;

  protected abstract mocody_createOne({ data }: { data: T }): Promise<T>;

  protected abstract mocody_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: Partial<T>;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T>;

  protected abstract mocody_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: Omit<IFuseQueryIndexOptions<TData, TSortKeyField>, "pagingParams">,
  ): Promise<T[]>;

  protected abstract mocody_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>>;

  protected abstract mocody_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T>;
  }): Promise<T>;
}
