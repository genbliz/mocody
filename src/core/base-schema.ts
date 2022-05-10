import Joi from "joi";

export interface IMocodyCoreEntityModel {
  id: string;
  featureEntity: string;
  dangerouslyExpireAt?: string;
  /**
   * @internal
   *  Do not set the value */
  dangerouslyExpireAtTTL?: string | number | Date;
}

export const coreSchemaDefinition = {
  id: Joi.string().required().min(5).max(512),
  featureEntity: Joi.string().required().min(2).max(256),
  dangerouslyExpireAt: [Joi.string().isoDate().strict(false), Joi.any().strip()],
  dangerouslyExpireAtTTL: Joi.any().strip(),
} as const;
