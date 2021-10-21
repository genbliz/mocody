type RequireAtLeastOneBase<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    /* https://stackoverflow.com/questions/40510611/typescript-interface-require-one-of-two-properties-to-exist*/
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];
type RequireAtLeastOne<T> = RequireAtLeastOneBase<T, keyof T>;

type TypeFallBackStringOnly<T> = Extract<T, string>;
type TypeFallBack<T> = undefined extends T ? Exclude<T, undefined> : T;
type TypeFallBackArray<T> = number extends T ? number[] : string extends T ? string[] : T;

export type IMocodyKeyConditionParams<T = string> = {
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

export type IMocodyQueryConditionParams<T = any> = IMocodyKeyConditionParams<T> & {
  $ne?: TypeFallBack<T> | null;
  $in?: TypeFallBackArray<T>;
  $nin?: TypeFallBackArray<T>;
  $exists?: boolean;
  $not?: IMocodyKeyConditionParams<T>;
  $elemMatch?: { $in: TypeFallBackArray<T> };
  //
  $contains?: TypeFallBackStringOnly<T>;
  $notContains?: TypeFallBackStringOnly<T>;
  $nestedMatch?: QueryKeyConditionBasic<RequireAtLeastOne<T>>;
};

type QueryPartialAll<T> = {
  [P in keyof T]: T[P] | IMocodyQueryConditionParams<T[P]>;
};

type QueryKeyConditionBasic<T> = {
  [P in keyof T]: T[P] | IMocodyKeyConditionParams<T[P]>;
};

export interface IMocodyPagingResult<T> {
  nextPageHash: string | undefined;
  paginationResults: T;
  // count: number | undefined;
}

export type IMocodyPagingParams = {
  evaluationLimit?: number;
  nextPageHash?: string;
};

type IQueryDefOr<T> = { $or?: QueryPartialAll<RequireAtLeastOne<T>>[] };
type IQueryDefAnd<T> = { $and?: QueryPartialAll<RequireAtLeastOne<T>>[] };

export type IMocodyQueryDefinition<T> = QueryPartialAll<RequireAtLeastOne<T & IQueryDefOr<T> & IQueryDefAnd<T>>>;

export interface IMocodyQueryIndexOptions<T, TSortKeyField = string> {
  indexName: string;
  partitionKeyValue: string | number;
  sortKeyQuery?: IMocodyKeyConditionParams<TSortKeyField>;
  query?: IMocodyQueryDefinition<T>;
  fields?: (keyof T)[];
  excludeFields?: (keyof T)[];
  pagingParams?: IMocodyPagingParams;
  limit?: number | null;
  sort?: "asc" | "desc" | null;
}

export type IMocodyQueryIndexOptionsNoPaging<T, TSortKeyField = string> = Omit<
  IMocodyQueryIndexOptions<T, TSortKeyField>,
  "pagingParams"
>;

export interface IMocodyIndexDefinition<T> {
  indexName: string;
  partitionKeyFieldName: keyof T;
  sortKeyFieldName: keyof T;
  dataType: "N" | "S";
  projectionFieldsInclude?: (keyof T)[];
}

export type IMocodyFieldCondition<T> = { field: keyof T; equals: string | number }[];

interface IMocodyCreateTransactionPrepare<T> {
  kind: "create";
  data: T;
}

interface IMocodyUpdateTransactionPrepare<T> {
  kind: "update";
  data: T;
  dataId: string;
}

interface IMocodyDeleteTransactionPrepare<T> {
  kind: "delete";
  dataId: string;
}

export type IMocodyTransactionPrepare<T> =
  | IMocodyCreateTransactionPrepare<T>
  | IMocodyUpdateTransactionPrepare<T>
  | IMocodyDeleteTransactionPrepare<T>;

interface IMocodyPreparedCreateTransaction {
  kind: "create";
  tableName: string;
  data: Record<string, any>;
  partitionKeyFieldName: "id";
}

interface IMocodyPreparedUpdateTransaction {
  kind: "update";
  tableName: string;
  data: Record<string, any>;
  keyQuery: Record<string, any>;
  partitionKeyFieldName: "id";
}

interface IMocodyPreparedDeleteTransaction {
  kind: "delete";
  tableName: string;
  keyQuery: Record<string, any>;
  partitionKeyFieldName: "id";
}

export type IMocodyPreparedTransaction =
  | IMocodyPreparedCreateTransaction
  | IMocodyPreparedUpdateTransaction
  | IMocodyPreparedDeleteTransaction;
