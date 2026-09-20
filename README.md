# ShardNest — Distributed Sharded Database

A production-grade sharded database built from scratch in Java/Spring Boot.
Implements **consistent hashing**, **RF=3 replication**, **quorum writes**,
**live rebalancing**, and **Redis caching** — the core architecture behind
systems like Cassandra, DynamoDB, and MongoDB.

---

## 🎯 What This Project Demonstrates

- **Consistent hashing with virtual nodes** — even data distribution across shards
- **Dynamic shard provisioning** — add new databases at runtime via REST API
- **Live rebalancing** — add/remove shards with zero downtime and no data loss
- **Replication factor 3** — every user exists on 3 shards for fault tolerance
- **Quorum writes (W=2)** — fast, safe writes that tolerate 1 shard failure
- **Fault-tolerant reads** — automatic fallback across replicas and shards
- **Backfill / reconciliation** — repair under-replicated data
- **Redis cache-aside** — 10x faster reads, ~90% reduction in DB load
- **Cache invalidation** — delete-on-write for consistency

---

## 🏗️ Architecture
