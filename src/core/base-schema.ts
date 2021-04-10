import Joi from "joi";

export interface IMocodyCoreEntityModel {
  id: string;
  featureEntity: string;
}

export const coreSchemaDefinition = {
  id: Joi.string().required().min(5).max(512),
  featureEntity: Joi.string().required().min(2).max(256),
} as const;
