import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { BunAdapter } from "@bull-board/bun";
import { Queue as QueueMQ } from "bullmq";
import { bullMQConnection, redisClient } from "./redis";

const createQueueMQ = (name: string) =>
  new QueueMQ(name, { connection: bullMQConnection });

const QUEUE_CONFIG_KEY = "bullmq:queues";

const serverAdapter = new BunAdapter();
serverAdapter.setBasePath("/ui");

interface QueueConfig {
  name: string;
  description?: string;
}

type JsonBody = Record<string, unknown>;

const queueAdapterMap = new Map<string, BullMQAdapter>();

const normalizeQueueName = (name: string): string => name.trim();

const toQueueAdapter = (name: string): BullMQAdapter =>
  new BullMQAdapter(createQueueMQ(name));

const jsonError = (message: string, status = 400): Response =>
  Response.json({ ok: false, message }, { status });

const parseJsonBody = async (request: Request): Promise<JsonBody | null> => {
  try {
    const body: unknown = await request.json();
    if (typeof body !== "object" || body === null || Array.isArray(body)) {
      return null;
    }
    return body as JsonBody;
  } catch {
    return null;
  }
};

const normalizeQueueConfig = (item: unknown): QueueConfig | null => {
  if (typeof item !== "object" || item === null) {
    return null;
  }

  const { name, description } = item as {
    name?: unknown;
    description?: unknown;
  };

  if (typeof name !== "string") {
    return null;
  }

  const normalizedName = normalizeQueueName(name);
  if (!normalizedName) {
    return null;
  }

  const config: QueueConfig = { name: normalizedName };
  if (typeof description === "string" && description.trim()) {
    config.description = description.trim();
  }

  return config;
};

const parseQueueConfigFromHashField = (
  field: string,
  rawValue: string,
): QueueConfig | null => {
  const normalizedName = normalizeQueueName(field);
  if (!normalizedName) {
    return null;
  }

  try {
    const parsed: unknown = JSON.parse(rawValue);
    const normalized = normalizeQueueConfig(parsed);
    if (!normalized) {
      return { name: normalizedName };
    }
    return {
      name: normalizedName,
      ...(normalized.description
        ? { description: normalized.description }
        : {}),
    };
  } catch {
    return { name: normalizedName };
  }
};

const loadQueueConfigsFromRedis = async (): Promise<QueueConfig[]> => {
  try {
    const record = await redisClient.hgetall(QUEUE_CONFIG_KEY);

    const configMap = new Map<string, QueueConfig>();
    for (const [field, rawValue] of Object.entries(record)) {
      const normalized = parseQueueConfigFromHashField(field, rawValue);
      if (normalized) {
        configMap.set(normalized.name, normalized);
      }
    }

    return [...configMap.values()];
  } catch (err) {
    console.error("Failed to parse queue configs from Redis:", err);
    return [];
  }
};

const startupQueueConfigs = await loadQueueConfigsFromRedis();
const startupQueueAdapters = startupQueueConfigs.map((config) => {
  const adapter = toQueueAdapter(config.name);
  queueAdapterMap.set(config.name, adapter);
  return adapter;
});

const { addQueue, removeQueue } = createBullBoard({
  queues: startupQueueAdapters,
  serverAdapter,
  options: {
    uiConfig: {
      boardTitle: "工作流监控面板",
    },
  },
});

const registerQueueAdapter = (name: string): void => {
  const normalizedName = normalizeQueueName(name);
  if (!normalizedName || queueAdapterMap.has(normalizedName)) {
    return;
  }

  const adapter = toQueueAdapter(normalizedName);
  queueAdapterMap.set(normalizedName, adapter);
  addQueue(adapter);
};

const unregisterQueueAdapter = (name: string): void => {
  const normalizedName = normalizeQueueName(name);
  const adapter = queueAdapterMap.get(normalizedName);
  if (!adapter) {
    return;
  }

  removeQueue(adapter);
  queueAdapterMap.delete(normalizedName);
};

const queueExistsInRedis = async (name: string): Promise<boolean> => {
  const queueName = normalizeQueueName(name);
  if (!queueName) {
    return false;
  }
  return redisClient.hexists(QUEUE_CONFIG_KEY, queueName);
};

const createQueue = async (input: QueueConfig): Promise<QueueConfig> => {
  const normalizedName = normalizeQueueName(input.name);
  if (!normalizedName) {
    throw new Error("Queue name is required");
  }
  if (await redisClient.hexists(QUEUE_CONFIG_KEY, normalizedName)) {
    throw new Error(`Queue ${normalizedName} already exists`);
  }

  const nextConfig: QueueConfig = { name: normalizedName };
  if (input.description?.trim()) {
    nextConfig.description = input.description.trim();
  }

  await redisClient.hset(QUEUE_CONFIG_KEY, {
    [normalizedName]: JSON.stringify(nextConfig),
  });
  registerQueueAdapter(normalizedName);
  return nextConfig;
};

const updateQueue = async (
  name: string,
  patch: { description?: string },
): Promise<QueueConfig> => {
  const queueName = normalizeQueueName(name);
  const rawConfig = await redisClient.hget(QUEUE_CONFIG_KEY, queueName);
  const current = rawConfig
    ? parseQueueConfigFromHashField(queueName, rawConfig)
    : null;
  if (!current) {
    throw new Error(`Queue ${queueName} not found`);
  }

  const nextConfig: QueueConfig = { name: queueName };
  if (typeof patch.description === "string" && patch.description.trim()) {
    nextConfig.description = patch.description.trim();
  } else if (current.description) {
    nextConfig.description = current.description;
  }

  await redisClient.hset(QUEUE_CONFIG_KEY, {
    [queueName]: JSON.stringify(nextConfig),
  });
  return nextConfig;
};

const deleteQueue = async (name: string): Promise<void> => {
  const queueName = normalizeQueueName(name);
  const deleted = await redisClient.hdel(QUEUE_CONFIG_KEY, queueName);
  if (deleted === 0) {
    throw new Error(`Queue ${queueName} not found`);
  }

  unregisterQueueAdapter(queueName);
};

const syncQueuesFromRedis = async (): Promise<QueueConfig[]> => {
  const latestConfigs = await loadQueueConfigsFromRedis();

  const latestNameSet = new Set(latestConfigs.map((item) => item.name));

  for (const name of [...queueAdapterMap.keys()]) {
    if (!latestNameSet.has(name)) {
      unregisterQueueAdapter(name);
    }
  }

  for (const config of latestConfigs) {
    registerQueueAdapter(config.name);
  }

  return latestConfigs;
};

// Get bull-board routes
const bullBoardRoutes = serverAdapter.getRoutes();

const { PORT, REDIS_PORT } = Bun.env;

// Start Bun server with routes
Bun.serve({
  port: PORT ? parseInt(PORT) : 3000,
  routes: {
    // Custom health check route
    "/health": {
      GET: () =>
        Response.json({ status: "ok", timestamp: new Date().toISOString() }),
    },
    "/queues": {
      GET: async () => {
        const configs = await loadQueueConfigsFromRedis();
        return Response.json({ ok: true, data: configs });
      },
      POST: async (request) => {
        const body = await parseJsonBody(request);
        if (!body) {
          return jsonError("Invalid JSON body", 400);
        }

        const config = normalizeQueueConfig(body);
        if (!config) {
          return jsonError("name is required", 400);
        }

        try {
          const created = await createQueue(config);
          return Response.json({ ok: true, data: created }, { status: 201 });
        } catch (err) {
          const message =
            err instanceof Error ? err.message : "Create queue failed";
          return jsonError(message, 409);
        }
      },
    },
    "/queues/sync": {
      POST: async () => {
        const synced = await syncQueuesFromRedis();
        return Response.json({ ok: true, data: synced });
      },
    },
    "/queues/:name": {
      GET: async (request) => {
        const queueName = normalizeQueueName(request.params.name ?? "");
        const configs = await loadQueueConfigsFromRedis();
        const queue = configs.find((item) => item.name === queueName);
        if (!queue) {
          return jsonError("Queue not found", 404);
        }
        return Response.json({ ok: true, data: queue });
      },
      PATCH: async (request) => {
        const queueName = normalizeQueueName(request.params.name ?? "");
        const body = await parseJsonBody(request);
        if (!body) {
          return jsonError("Invalid JSON body", 400);
        }
        if ("name" in body) {
          return jsonError("Queue rename is not supported", 400);
        }

        try {
          const updated = await updateQueue(queueName, {
            description:
              typeof body.description === "string"
                ? body.description
                : undefined,
          });
          return Response.json({ ok: true, data: updated });
        } catch (err) {
          const message =
            err instanceof Error ? err.message : "Update queue failed";
          return jsonError(message, 404);
        }
      },
      DELETE: async (request) => {
        const queueName = normalizeQueueName(request.params.name ?? "");
        try {
          await deleteQueue(queueName);
          return Response.json({ ok: true });
        } catch (err) {
          const message =
            err instanceof Error ? err.message : "Delete queue failed";
          return jsonError(message, 404);
        }
      },
    },
    "/queues/:name/jobs": {
      POST: async (request) => {
        const queueName = normalizeQueueName(request.params.name ?? "");
        if (!(await queueExistsInRedis(queueName))) {
          return jsonError("Queue not found", 404);
        }

        const body = await parseJsonBody(request);
        if (!body) {
          return jsonError("Invalid JSON body", 400);
        }

        const jobName =
          typeof body.jobName === "string" && body.jobName.trim()
            ? body.jobName.trim()
            : "job";
        const data =
          typeof body.data === "object" && body.data !== null ? body.data : {};

        const queue = createQueueMQ(queueName);
        const job = await queue.add(jobName, data);
        await queue.close();
        return Response.json({ ok: true, data: { id: job.id, queueName } });
      },
    },
    // Spread bull-board routes
    ...bullBoardRoutes,
  },
});

/* eslint-disable no-console */
console.log("Running on http://localhost:3000...");
console.log(`For the UI, open http://localhost:3000/ui`);
console.log(
  `Make sure Redis is running on port ${REDIS_PORT || 6379} by default`,
);
console.log("Queue CRUD API:");
console.log("  GET    /queues");
console.log("  POST   /queues");
console.log("  GET    /queues/:name");
console.log("  PATCH  /queues/:name");
console.log("  DELETE /queues/:name");
console.log("  POST   /queues/sync");
console.log("  POST   /queues/:name/jobs");
/* eslint-enable no-console */
