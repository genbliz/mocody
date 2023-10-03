type RequireAtLeastOneBase<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    /* https://stackoverflow.com/questions/40510611/typescript-interface-require-one-of-two-properties-to-exist*/
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];
type RequireAtLeastOne<T> = RequireAtLeastOneBase<T, keyof T>;

type TypeFallBackStringOnly<T> = T extends string ? Extract<T, string> : never;
type TypeFallBack<T> = undefined extends T ? Exclude<T, undefined> : T;
type TypeFallBackArray<T> = number extends T ? number[] : string extends T ? string[] : T;
type TypeFallBackArrayAdvanced<T> = number extends T
  ? number[]
  : string extends T
  ? string[]
  : Extract<T, string>[] | Extract<T, number>[];

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
  $in?: TypeFallBackArrayAdvanced<T>;
  $nin?: TypeFallBackArrayAdvanced<T>;
  $exists?: boolean;
  $elemMatch?: { $in: TypeFallBackArray<T> };
  //
  $contains?: TypeFallBackStringOnly<T>;
  $notContains?: TypeFallBackStringOnly<T>;
  $nestedMatch?: IQueryNested<RequireAtLeastOne<T>>;
};

export type IMocodyQueryNestedParams<T = any> = IMocodyKeyConditionParams<T> & {
  $contains?: TypeFallBackStringOnly<T>;
  $ne?: TypeFallBack<T> | null;
  $in?: TypeFallBackArrayAdvanced<T>;
  $nin?: TypeFallBackArrayAdvanced<T>;
  $exists?: boolean;
};

type IQueryAll<T> = {
  [P in keyof T]: T[P] | IMocodyQueryConditionParams<T[P]>;
};

type IQueryNested<T> = {
  [P in keyof T]: T[P] | IMocodyQueryNestedParams<T[P]>;
};

export interface IMocodyPagingResult<T> {
  nextPageHash: string | undefined | null;
  paginationResults: T;
  // count: number | undefined;
}

export type IMocodyPagingParams = {
  evaluationLimit?: number | undefined | null;
  nextPageHash?: string | undefined | null;
};

type IQueryDefOr<T> = { $or?: IQueryAll<RequireAtLeastOne<T>>[] };
type IQueryDefAnd<T> = { $and?: IQueryAll<RequireAtLeastOne<T>>[] };

export type IMocodyQueryDefinition<T> = IQueryAll<RequireAtLeastOne<T & IQueryDefOr<T> & IQueryDefAnd<T>>>;

export interface IMocodyQueryIndexOptions<T, TSortKeyField = string> {
  indexName: string;
  partitionKeyValue: string | number;
  sortKeyQuery?: IMocodyKeyConditionParams<TSortKeyField> | undefined | null;
  query?: IMocodyQueryDefinition<T> | undefined | null;
  fields?: (keyof T)[] | undefined | null;
  excludeFields?: (keyof T)[] | undefined | null;
  pagingParams?: IMocodyPagingParams | undefined | null;
  limit?: number | undefined | null;
  sort?: "asc" | "desc" | undefined | null;
}

export type IMocodyQueryIndexOptionsNoPaging<T, TSortKeyField = string> = Omit<
  IMocodyQueryIndexOptions<T, TSortKeyField>,
  "pagingParams"
>;

export interface IMocodyIndexDefinition<T> {
  indexName: string;
  partitionKeyFieldName: keyof T;
  sortKeyFieldName: keyof T;
  dataTypes: {
    partitionKeyDataType: "N" | "S";
    sortKeyDataType: "N" | "S";
  };
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
