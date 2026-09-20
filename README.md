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

---


```bash

📦 Tech Stack
Layer	Technology
Language	Java 23
Framework	Spring Boot 4.1
ORM	Hibernate / Spring Data JPA
Databases	MySQL 9.2 (4 shards)
Cache	Redis 7 + Lettuce
Serialization	Jackson 3
Hash Function	Murmur3 (Guava)
IDs	ULID
Build	Maven

🙏 Acknowledgments
Inspired by:

Amazon Dynamo paper (2007) — consistent hashing + replication

Apache Cassandra — VNodes + tunable consistency

MongoDB — replica sets

Martin Kleppmann's "Designing Data-Intensive Applications"

📬 Contact
Author: [Bhuvan V]

Email: [bhuvanvachar0123@gmail.com]

LinkedIn: [linkedin.com/in/yourprofile](https://www.linkedin.com/in/bhuvan-v-188284246]


---

