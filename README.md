# tf-mq

## 安装依赖

    bun install

## 启动服务

    bun run src/index.ts

服务默认地址:

- API: http://localhost:3000
- Bull Board UI: http://localhost:3000/ui

## Queue CRUD API 示例

### 1) 查询所有 Queue

    curl -X GET http://localhost:3000/queues

### 2) 创建 Queue

    curl -X POST http://localhost:3000/queues \
      -H "content-type: application/json" \
      -d '{"name":"demo-queue","description":"demo"}'

### 3) 查询单个 Queue

    curl -X GET http://localhost:3000/queues/demo-queue

### 4) 更新 Queue（仅 description）

    curl -X PATCH http://localhost:3000/queues/demo-queue \
      -H "content-type: application/json" \
      -d '{"description":"demo-updated"}'

### 5) 删除 Queue

    curl -X DELETE http://localhost:3000/queues/demo-queue

### 6) 从 Redis 同步 Queue

    curl -X POST http://localhost:3000/queues/sync

### 7) 向指定 Queue 添加任务

    curl -X POST http://localhost:3000/queues/demo-queue/jobs \
      -H "content-type: application/json" \
      -d '{"jobName":"sync-demo","data":{"issueId":"123"}}'

## Redis 持久化说明

- Queue 配置存储在 Redis 键: bullmq:queues
- 服务启动时会自动从 Redis 读取队列配置并注册到 bull-board
- 调用创建、更新、删除接口时会同步写回 Redis
- 调用同步接口可强制从 Redis 重新加载配置

This project was created using bun init in bun v1.3.11. Bun is a fast all-in-one JavaScript runtime.

## Docker 构建镜像

```shell
docker buildx build --platform linux/amd64 -t tf-mq:1.0  --output type=image,push=true,registry.insecure=true .
```
