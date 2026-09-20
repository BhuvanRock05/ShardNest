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



🔌 API Reference
User APIs
Method	Endpoint	Description
POST	/api/user/create	Create user (replicated to 3 shards)
GET	/api/user/{id}	Read user (cache-aside)
DELETE	/api/user/delete/{id}	Delete user from all replicas
Admin APIs — Shards
Method	Endpoint	Description
POST	/api/admin/shard/add	Add shard to ring and rebalance
POST	/api/admin/shard/remove	Remove shard and drain data
POST	/api/admin/shard/backfill	Repair under-replicated users
GET	/api/admin/shard/list	List all shards
GET	/api/admin/shard/status	Rebalance status
Admin APIs — DataSource Provisioning
Method	Endpoint	Description
POST	/api/admin/datasource/provision	Create DB + tables + DataSource at runtime



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
Author: [Your Name]

Email: [your.email@example.com]

LinkedIn: [linkedin.com/in/yourprofile]

text

---

## 📌 Placeholders to Replace

Before you commit, replace these in the file:

| Placeholder | Replace With |
|-------------|--------------|
| `[Your Name]` | Your full name |
| `[your.email@example.com]` | Your email |
| `[linkedin.com/in/yourprofile]` | Your LinkedIn URL |
| `https://github.com/yourusername/shardNest.git` | Your actual GitHub repo URL |

---
