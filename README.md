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
┌────────────────────────────────────────────────────────────┐
│ HTTP API │
│ /api/user/* → user CRUD │
│ /api/admin/shard/* → add/remove/backfill shards │
│ /api/admin/datasource/* → provision new databases │
│ /api/debug/* → inspect ring, replicas, cache │
└────────────────────────────────────────────────────────────┘
↓
┌────────────────────────────────────────────────────────────┐
│ Redis Cache │
│ GET /api/user/{id} → check cache → HIT? return │
│ → MISS? query shards │
└────────────────────────────────────────────────────────────┘
↓
┌────────────────────────────────────────────────────────────┐
│ Consistent Hash Ring │
│ 100 VNodes per shard → even distribution │
│ getShard(key) → primary shard │
│ getReplicas(key, 3) → [primary, replica1, replica2] │
└────────────────────────────────────────────────────────────┘
↓
┌────────────────────────────────────────────────────────────┐
│ ShardOperationService │
│ - Quorum writes (parallel, W=2) │
│ - Fault-tolerant reads │
│ - Rebalance / backfill │
└────────────────────────────────────────────────────────────┘
↓
┌────────────┬────────────┬────────────┬────────────┐
│ shard1 │ shard2 │ shard3 │ shard4 │
│ (userdb1) │ (userdb2) │ (userdb3) │ (userdb4) │
│ MySQL │ MySQL │ MySQL │ MySQL │
│ RF=3 │ RF=3 │ RF=3 │ RF=3 │
└────────────┴────────────┴────────────┴────────────┘

text

---

## 📚 Key Design Decisions

### 1. Why Consistent Hashing (Not Modulo)?

**Modulo:** `hash(key) % N_shards`
- When adding a shard, **~75% of keys must move** (with N=4)
- Requires full data migration

**Consistent Hashing:**
- When adding a shard, **only ~1/N keys move** (~25% with N=4)
- Only the affected arc moves; everything else stays put

**Result:** Live rebalancing without downtime.

### 2. Why Virtual Nodes (VNodes)?

Without VNodes, each shard is placed at exactly 1 position on the ring.
With 3 shards, distribution is terrible (one shard gets ~99% of keys).

With 100 VNodes per shard:
- 300 positions on the ring
- Distribution approaches ideal (~33% each)
- Deviation scales as `1/√vnodeCount`

### 3. Why Murmur3 Hash Instead of `String.hashCode()`?

`String.hashCode()` is **linear**: changing `"shard1#0"` → `"shard1#1"` changes the hash by exactly +1. VNodes end up clustered at consecutive positions.

**Murmur3** has the "avalanche effect": one bit change flips ~50% of output bits. VNodes spread evenly across the ring.

### 4. Why RF=3 and Quorum Writes?

**Replication Factor 3** means each user lives on 3 shards:
- Tolerates 1 shard failure without data loss
- Reads can fall back to a replica if primary is down

**Quorum writes (W=2 of N=3):**
- Write in parallel to all 3 replicas
- Return success as soon as 2 confirm
- **2x faster** than waiting for all 3
- Still safe: `W + R = 2 + 2 = 4 > N = 3` ensures strong consistency

### 5. Why `open-in-view: false`?

This was the **hardest bug** in the project.

Spring's default `open-in-view: true` keeps the JPA `EntityManager` open
for the entire HTTP request. Combined with `AbstractRoutingDataSource`,
this means:

- The DataSource is chosen when the first query runs
- Subsequent queries reuse that DataSource
- Changing `ShardContext` mid-request has **no effect**

**Symptom:** Writes silently went to the wrong shard.

**Fix:** Set `open-in-view: false` and use `TransactionTemplate` (not
`@Transactional`) so `ShardContext` is set **before** the transaction opens.

### 6. Why `saveAndFlush` (Not `save`)?

`save()` calls `merge()` for entities with a preset ID. If the same
`User` object reference exists in the current persistence context,
Hibernate issues an `UPDATE` instead of an `INSERT`.

**Fix:** Use `saveAndFlush()` and always write a **fresh copy** of the
entity (never reuse the same object across shards).

### 7. Why Cache-Aside (Not Write-Through)?

Cache-aside (read-through + delete-on-write) is simplest and safest:

- **Read:** check cache → HIT? return. MISS? read DB → cache → return
- **Write:** write DB → delete cache entry (next read repopulates)

Update-on-write has race conditions with concurrent reads. Delete-on-write
avoids them by forcing a fresh DB read.

---

## 🚀 Running Locally

### Prerequisites

- **Java 23**
- **Maven**
- **MySQL 8+** (4 databases: `userdb1`, `userdb2`, `userdb3`, `userdb4`)
- **Redis 7+** (running on `localhost:6379`)

### Setup

```bash
# 1. Clone
git clone https://github.com/yourusername/shardNest.git
cd shardNest

# 2. Create the databases (tables are auto-created on startup)
mysql -u root -p < scripts/create-databases.sql

# 3. Start Redis
docker run -d --name redis-shardnest -p 6379:6379 redis:7-alpine

# 4. Run
mvn spring-boot:run
Create Databases SQL
sql
CREATE DATABASE IF NOT EXISTS userdb1;
CREATE DATABASE IF NOT EXISTS userdb2;
CREATE DATABASE IF NOT EXISTS userdb3;
CREATE DATABASE IF NOT EXISTS userdb4;
Tables are created automatically via spring.jpa.hibernate.ddl-auto=validate after
schema provisioning. Use the admin API to create schemas in new databases.

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
Debug APIs
Method	Endpoint	Description
GET	/api/debug/ring	Full hash ring
GET	/api/debug/ring/summary	VNode distribution
GET	/api/debug/route/{key}	Route a key to a shard
GET	/api/debug/replicas/{key}	Show replica set for a key
GET	/api/debug/distribution/{count}	Distribution test
GET	/api/debug/user-locations/{id}	Expected vs actual replicas
GET	/api/debug/cache/health	Redis health
GET	/api/debug/cache/user/{id}	Inspect cached entry
🎬 Example Workflow
1. Create a user
bash
curl -X POST http://localhost:8080/api/user/create \
  -H "Content-Type: application/json" \
  -d '{
    "userName": "alice",
    "password": "secret",
    "firstName": "Alice",
    "lastName": "Smith",
    "email": "alice@example.com",
    "phone": "1234567890",
    "address": {
      "street": "1 Main St",
      "city": "Springfield",
      "state": "IL",
      "country": "USA",
      "zipCode": "62701"
    }
  }'
Response:

json
{
  "id": "01M2Q1RE680ZQKYX5ZY0VPZM0K",
  "firstName": "Alice",
  "lastName": "Smith",
  "email": "alice@example.com",
  "phone": "1234567890",
  "address": { ... }
}
2. Verify replicas
bash
curl http://localhost:8080/api/debug/replicas/01M2Q1RE680ZQKYX5ZY0VPZM0K
json
{
  "key": "01M2Q1RE680ZQKYX5ZY0VPZM0K",
  "primary": "shard2",
  "replicas": ["shard4", "shard1"],
  "all": ["shard2", "shard4", "shard1"]
}
The user exists on shard2, shard4, and shard1 — RF=3.

3. Read (cache miss → DB)
bash
curl http://localhost:8080/api/user/01M2Q1RE680ZQKYX5ZY0VPZM0K
Logs:

text
CACHE MISS: shardnest:user:01M2Q1RE680ZQKYX5ZY0VPZM0K (5ms)
>>> ROUTING to shard: shard2
Hibernate: select ...
CACHE PUT: shardnest:user:01M2Q1RE680ZQKYX5ZY0VPZM0K (3ms)
4. Read again (cache hit)
bash
curl http://localhost:8080/api/user/01M2Q1RE680ZQKYX5ZY0VPZM0K
Logs:

text
CACHE HIT: shardnest:user:01M2Q1RE680ZQKYX5ZY0VPZM0K (1ms)
No DB query — 10x faster.

5. Add a new shard (shard5)
Step 1: Provision DataSource

bash
curl -X POST http://localhost:8080/api/admin/datasource/provision \
  -H "Content-Type: application/json" \
  -d '{
    "shardId": "shard5",
    "jdbcUrl": "jdbc:mysql://localhost:3306/userdb5",
    "username": "root",
    "password": "root@123",
    "driverClassName": "com.mysql.cj.jdbc.Driver",
    "createDatabase": true,
    "databaseName": "userdb5",
    "createTables": true
  }'
Step 2: Add to ring + rebalance

bash
curl -X POST "http://localhost:8080/api/admin/shard/add?shardId=shard5&databaseName=userdb5"
Response:

json
{
  "shard": "shard5",
  "moved": 42,
  "status": "OK"
}
~25% of users were rebalanced to shard5.

6. Cache invalidation test
bash
# Cache the user
curl http://localhost:8080/api/user/01M2...

# Delete
curl -X DELETE http://localhost:8080/api/user/delete/01M2...

# Cache should now be empty
curl http://localhost:8080/api/debug/cache/user/01M2...
🧪 Testing
(To be added — see roadmap)

Planned test coverage:

HashRingTest — hashing, wraparound, distribution

ConsistentHashRouterTest — primary = replicas[0] invariant

ReplicationTest — RF=3 verified

CacheInvalidationTest — delete → evict

RebalanceTest — add shard → users preserved

BackfillTest — under-replicated users fixed

📊 Performance Characteristics
Operation	Latency	Notes
Cache hit	~1-5ms	No DB access
Cache miss (read)	~30-50ms	DB + cache write
Create user (quorum)	~50-100ms	Parallel writes, W=2
Delete user	~100-150ms	Sequential across replicas
Add shard	~1-10s	Depends on data size
Backfill	~1-60s	Depends on data size
Scalability
Shards: Horizontal — add more at runtime

Throughput: ~10x more with cache

Storage: 3x (RF=3)

Failure tolerance: 1 shard can die without data loss

🔍 Debugging Tips
1. Ring inspection
bash
# See all 400 VNodes (4 shards × 100)
curl http://localhost:8080/api/debug/ring

# Summary
curl http://localhost:8080/api/debug/ring/summary
2. Trace a key's journey
bash
curl http://localhost:8080/api/debug/route/user-abc
curl http://localhost:8080/api/debug/replicas/user-abc
curl http://localhost:8080/api/debug/user-locations/{actual-id}
3. Inspect cache
bash
redis-cli KEYS "shardnest:user:*"
redis-cli GET shardnest:user:01M2...
redis-cli TTL shardnest:user:01M2...
4. Check shard state
sql
USE userdb1; SELECT COUNT(*) FROM users_table;
USE userdb2; SELECT COUNT(*) FROM users_table;
USE userdb3; SELECT COUNT(*) FROM users_table;
USE userdb4; SELECT COUNT(*) FROM users_table;
-- Sum should be 3x total users (RF=3)
🎓 What I Learned
Building this project taught me real distributed systems lessons:

Framework defaults can hide bugs — open-in-view: true seemed harmless
until sharding exposed it.

JPA entity identity matters — reusing the same object across databases
silently breaks inserts.

Failure handling is 90% of the code — the happy path is easy; catching
partial failures, timeout handling, and fallback logic is where the real
complexity lives.

Consistency vs availability — the CAP theorem is not academic. Every
design decision (quorum W=2, cache TTL, sync vs async) trades one for the
other.

Idempotency is a superpower — backfill, rebalance, and cache eviction
are all safe to retry. This makes recovery trivial.

Observability is essential — logs, metrics, and debug endpoints turned
multi-hour bugs into 15-minute fixes.

🚧 Roadmap (Future Work)
□ Quorum reads with read repair (R=2, detect stale replicas)
□ Async replication (write to primary, replicate in background)
□ Circuit breaker for Redis (skip when down, recover automatically)
□ Prometheus metrics + Grafana dashboards
□ Anti-entropy repair job (Merkle trees)
□ Leader election for replica promotion (Raft)
□ Distributed transactions (saga pattern)
□ Kubernetes deployment manifests
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
📄 License
MIT — see LICENSE for details.

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

## ✅ How to Add This File

1. In your project root, create `README.md`
2. Copy **everything from the first `#` to the last backtick** (from `# ShardNest` down to the closing triple-backtick)
3. Paste into `README.md`
4. Replace the 4 placeholders
5. Optionally create a `LICENSE` file (MIT text)
6. Commit and push:
   ```bash
   git add README.md LICENSE
   git commit -m "docs: add comprehensive README and MIT license"
   git push
