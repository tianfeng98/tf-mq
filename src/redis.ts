import { createBunRedisClient, type BunRedisRawClient } from "bullmq";
import { RedisClient } from "bun";

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = Bun.env;

export const redisClient = new RedisClient(
  `redis://admin:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}`,
);
export const bullMQConnection = createBunRedisClient(
  redisClient as BunRedisRawClient,
);
