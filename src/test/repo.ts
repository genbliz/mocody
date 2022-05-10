import { UtilService } from "../helpers/util-service";
import type { IMocodyQueryDefinition } from "../type";
import { BaseRepository } from "./base-repo-mongo";
import Joi from "joi";
import faker from "faker";

export interface IPayment {
  amount: number;
  category: string;
  skills: string[];
  tenantId: string;
  invoiceId: string;
  transactionId?: string;
  remark: string;
  bill?: { amount: number; date: string; remark: string };
}

// const query: IQueryDefinition<IPayment> = { amount: 0, category: "" };
// if (query) {
// }

const _searchTerm = "";

const tenantId = "c4a3bdd6-289b-cb23-09a7-915adad9ea4f";
const definedSkills = ["JavaScript", "Node", "C#", "Mongo", "React", "Angular"];

export const paramOptions: IMocodyQueryDefinition<IPayment> = {
  $or: [
    // { category: { $contains: _searchTerm } },
    { invoiceId: { $contains: _searchTerm } },
    { transactionId: { $contains: _searchTerm } },
    { remark: { $contains: _searchTerm } },
  ],
};

const schemaSubDef = {
  category: Joi.string().required(),
  tenantId: Joi.string().required(),
  skills: Joi.array().items(Joi.string()).required(),
  amount: Joi.number().min(1),
  invoiceId: Joi.string().empty("").default(null).allow(null),
  transactionId: Joi.string().empty("").default(null).allow(null),
  remark: Joi.string().empty("").default(null).allow(null),
  bill: Joi.object({
    amount: Joi.number().min(1),
    date: Joi.string().required(),
    remark: Joi.string().required(),
  }),
};

const getRandom = () => UtilService.getRandomString(0);
const getRandomNumber = () => UtilService.getRandomInt(100, 10000);

export const DefinedIndexes = {
  featureEntity_tenantId: {
    dataType: "S",
    indexName: "featureEntity_tenantId_index",
    partitionKeyFieldName: "featureEntity",
    sortKeyFieldName: "tenantId",
  },
};

class MyRepositoryBase extends BaseRepository<IPayment> {
  private readonly featureEntityValue: string;
  constructor() {
    super({
      schemaSubDef,
      secondaryIndexOptions: [DefinedIndexes.featureEntity_tenantId] as any[],
      featureEntityValue: "payments",
    });
    this.featureEntityValue = "payments";
  }

  async getIt() {
    return await this.mocody_getManyByIndex({
      indexName: DefinedIndexes.featureEntity_tenantId.indexName,
      partitionKeyValue: this.featureEntityValue,
      query: {
        category: { $eq: null },
        // skills: {
        //   $elemMatch: {
        //     $in: ["889"],
        //   },
        // },
        // bill: { amount: 900 },
        bill: {
          $nestedMatch: {
            amount: { $between: [25000, 40000] },
            // remark: { $beginsWith: "Data" },
          },
        },
      },
    });
  }

  async create() {
    const data = {
      tenantId,
      amount: getRandomNumber(),
      category: getRandom(),
      skills: Array.from(
        new Set([
          //
          faker.helpers.randomize(definedSkills),
          faker.helpers.randomize(definedSkills),
          faker.helpers.randomize(definedSkills),
          faker.helpers.randomize(definedSkills),
        ]),
      ),
      remark: getRandom(),
      transactionId: getRandom(),
      invoiceId: getRandom(),
      bill: {
        amount: getRandomNumber(),
        date: faker.date.between(new Date("2020-03-01"), new Date()).toISOString(),
        remark: faker.random.word(),
      },
    };
    const result = await this.mocody_createOne({ data });
    console.log(result);
  }

  async update() {
    await this.mocody_updateOne({
      dataId: "",
      updateData: {
        amount: getRandomNumber(),
        // category: getRandom().toString(),
        invoiceId: getRandom().toString(),
        remark: getRandom().toString(),
        transactionId: getRandom().toString(),
      },
      withCondition: [
        {
          field: "category",
          equals: "hello",
        },
      ],
    });
  }
}

export const MyRepository = new MyRepositoryBase();
