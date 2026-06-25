import { createBunRedisClient, type BunRedisRawClient } from "bullmq";
import { RedisClient } from "bun";

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_USERNAME } = Bun.env;

const urlObj = new URL(`redis://${REDIS_HOST}:${REDIS_PORT}`);
if (REDIS_USERNAME) {
  urlObj.username = REDIS_USERNAME;
}
if (REDIS_PASSWORD) {
  urlObj.password = REDIS_PASSWORD;
}

export const redisClient = new RedisClient(urlObj.toString());
export const bullMQConnection = createBunRedisClient(
  redisClient as BunRedisRawClient,
);
