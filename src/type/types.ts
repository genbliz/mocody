type RequireAtLeastOneBase<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    /* https://stackoverflow.com/questions/40510611/typescript-interface-require-one-of-two-properties-to-exist*/
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];
type RequireAtLeastOne<T> = RequireAtLeastOneBase<T, keyof T>;

type TypeFallBackStringOnly<T> = Extract<T, string>;
type TypeFallBack<T> = undefined extends T ? Exclude<T, undefined> : T;
type TypeFallBackArray<T> = number extends T ? number[] : string extends T ? string[] : T;

export type IFuseKeyConditionParams<T = string> = {
  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.KeyConditions.html
  $eq?: TypeFallBack<T> | null;
  $gt?: TypeFallBack<T>;
  $gte?: TypeFallBack<T>;
  $lt?: TypeFallBack<T>;
  $lte?: TypeFallBack<T>;
  //
  $between?: [TypeFallBack<T>, TypeFallBack<T>];
  $beginsWith?: TypeFallBackStringOnly<T>;
};

export type IFuseQueryConditionParams<T = any> = IFuseKeyConditionParams<T> & {
  $ne?: TypeFallBack<T> | null;
  $in?: TypeFallBackArray<T>;
  $nin?: TypeFallBackArray<T>;
  $exists?: boolean;
  $not?: IFuseKeyConditionParams<T>;
  $elemMatch?: { $in: TypeFallBackArray<T> };
  //
  $contains?: TypeFallBackStringOnly<T>;
  $notContains?: TypeFallBackStringOnly<T>;
  $nestedMatch?: QueryKeyConditionBasic<RequireAtLeastOne<T>>;
};

type QueryPartialAll<T> = {
  [P in keyof T]: T[P] | IFuseQueryConditionParams<T[P]>;
};

type QueryKeyConditionBasic<T> = {
  [P in keyof T]: T[P] | IFuseKeyConditionParams<T[P]>;
};

export interface IFusePagingResult<T> {
  nextPageHash: string | undefined;
  mainResult: T;
  // count: number | undefined;
}

export type IFusePagingParams = {
  evaluationLimit?: number;
  nextPageHash?: string;
};

type IQueryDefOr<T> = { $or?: QueryPartialAll<RequireAtLeastOne<T>>[] };
type IQueryDefAnd<T> = { $and?: QueryPartialAll<RequireAtLeastOne<T>>[] };

export type IFuseQueryDefinition<T> = QueryPartialAll<RequireAtLeastOne<T & IQueryDefOr<T> & IQueryDefAnd<T>>>;

/*
export interface IFuseQueryParamOptions<T, ISortKeyObjField = any> {
  partitionKeyValue: string | number;
  sortKeyQuery?: QueryKeyConditionBasic<Required<ISortKeyObjField>>;
  query?: IFuseQueryDefinition<T>;
  fields?: (keyof T)[];
  pagingParams?: IFusePagingParams;
  limit?: number | null;
  sort?: "asc" | "desc" | null;
}
*/

export interface IFuseQueryIndexOptions<T, TSortKeyField = string> {
  indexName: string;
  partitionKeyValue: string | number;
  sortKeyQuery?: IFuseKeyConditionParams<TSortKeyField>;
  query?: IFuseQueryDefinition<T>;
  fields?: (keyof T)[];
  pagingParams?: IFusePagingParams;
  limit?: number | null;
  sort?: "asc" | "desc" | null;
}

export type IFuseQueryIndexOptionsNoPaging<T, TSortKeyField = string> = Omit<
  IFuseQueryIndexOptions<T, TSortKeyField>,
  "pagingParams"
>;

export interface IFuseIndexDefinition<T> {
  indexName: string;
  partitionKeyFieldName: keyof T;
  sortKeyFieldName: keyof T;
  dataType: "N" | "S";
  projectionFieldsInclude?: (keyof T)[];
}

export type IFuseFieldCondition<T> = { field: keyof T; equals: string | number }[];
