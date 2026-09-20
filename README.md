# ShardNest — Distributed Sharded Database

A distributed sharded database built from scratch using **Java and Spring Boot**.

ShardNest demonstrates the core concepts behind modern distributed data stores, including **consistent hashing, virtual nodes, sharding, replication, quorum-based writes, fault tolerance, live rebalancing, and Redis caching**.

---

## 🎯 What This Project Demonstrates

* **Consistent hashing with virtual nodes** — distributes data across shards while minimizing data movement when nodes are added or removed.
* **Dynamic shard provisioning** — add new database nodes at runtime through REST APIs.
* **Live rebalancing** — redistribute data when the cluster topology changes.
* **Replication factor 3 (RF=3)** — maintains three copies of each data item for fault tolerance.
* **Quorum writes (W=2)** — requires acknowledgements from at least two replicas before considering a write successful.
* **Quorum reads (R=2)** — reads from multiple replicas to improve reliability and consistency.
* **Fault-tolerant reads** — automatically falls back to other replicas when a node is unavailable.
* **Backfill / reconciliation** — repairs under-replicated or inconsistent data.
* **Redis cache-aside** — caches frequently accessed data to reduce database load.
* **Delete-on-write cache invalidation** — invalidates stale cache entries after successful database writes.
* **Dynamic node management** — supports adding and removing nodes from the cluster.
* **Observability and load testing** — designed to measure system behavior under distributed workloads.

---

## 🏗️ Architecture

```text
                           ┌─────────────────┐
                           │     Client      │
                           └────────┬────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │   ShardNest     │
                           │   Application   │
                           └────────┬────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
                     ▼                             ▼
              ┌─────────────┐              ┌─────────────────┐
              │    Redis    │              │ Consistent Hash │
              │    Cache    │              │      Ring       │
              └──────┬──────┘              └────────┬────────┘
                     │                              │
              Cache HIT │ MISS                      ▼
                     │                     ┌─────────────────┐
                     │                     │ Virtual Nodes   │
                     │                     └────────┬────────┘
                     │                              │
                     │                              ▼
                     │                     ┌─────────────────┐
                     │                     │ Physical Shard  │
                     │                     └────────┬────────┘
                     │                              │
                     │                     ┌────────┴────────┐
                     │                     │                 │
                     │                     ▼                 ▼
                     │                  Primary          Replicas
                     │                     │             ┌───┴───┐
                     │                     │             │       │
                     │                     ▼             ▼       ▼
                     │                  MySQL          MySQL   MySQL
                     │
                     └─────────────── Cache-Aside
```

---

## 🔄 Request Flow

### Read Flow

ShardNest follows a **cache-aside** strategy for reads.

```text
Client
  │
  ▼
ShardNest
  │
  ▼
Redis
  │
  ├── HIT ───────────────► Return data
  │
  └── MISS
       │
       ▼
  Consistent Hashing
       │
       ▼
    Shard
       │
       ▼
   Database
       │
       ▼
  Store in Redis
       │
       ▼
  Return data
```

### Write Flow

```text
Client
  │
  ▼
ShardNest
  │
  ▼
Consistent Hashing
  │
  ▼
Primary + Replicas
  │
  ▼
Write to replicas
  │
  ▼
Wait for W = 2 acknowledgements
  │
  ▼
Write successful
  │
  ▼
Delete Redis cache entry
```

The database remains the **source of truth**, while Redis acts as a cache.

---

## 🔁 Replication

ShardNest uses a **Replication Factor of 3**.

For a particular shard:

```text
                 Shard 1
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Node 1     Node 2    Node 3
       PRIMARY    REPLICA   REPLICA
          │         │         │
          ▼         ▼         ▼
        MySQL     MySQL     MySQL
        users     users     users
```

Each replica contains the same logical data for that shard.

### Replication Factor

```text
N = 3
```

means that each piece of data has three copies.

### Write Quorum

```text
W = 2
```

means at least two replicas must acknowledge a write.

For example:

```text
Node 1 → ACK ✅
Node 2 → ACK ✅
Node 3 → Timeout ❌

2 ACKs >= W(2)

Write successful ✅
```

This allows the system to tolerate a single replica failure while still accepting writes.

### Read Quorum

```text
R = 2
```

means the system waits for responses from at least two replicas for a quorum read.

A commonly used quorum relationship is:

```text
W + R > N
```

For ShardNest:

```text
2 + 2 > 3
```

---

## 🧩 Consistent Hashing

ShardNest uses a consistent hashing ring to determine which shard should store a particular key.

```text
                       Hash Ring
                  ┌─────────────────┐
                  │                 │
              Node 1              Node 2
                  │                 │
                  │                 │
              Node 4              Node 3
                  │                 │
                  └─────────────────┘
```

A key is hashed onto the ring and assigned to the next available node in the clockwise direction.

### Virtual Nodes

Each physical node owns multiple positions on the ring:

```text
Physical Node 1
 ├── vnode-1
 ├── vnode-2
 ├── vnode-3
 └── ...

Physical Node 2
 ├── vnode-1
 ├── vnode-2
 ├── vnode-3
 └── ...
```

Virtual nodes improve data distribution and reduce hotspots.

---

## ⚖️ Live Rebalancing

When a new node is added:

```text
Before:

Node 1 ───── Node 2 ───── Node 3
```

After adding Node 4:

```text
Node 1 ─── Node 2 ─── Node 4 ─── Node 3
```

Only the affected key ranges need to move instead of redistributing the entire dataset.

### Rebalancing Flow

```text
Add Node
   │
   ▼
Create DataSource
   │
   ▼
Register Node
   │
   ▼
Add Virtual Nodes
   │
   ▼
Identify affected key ranges
   │
   ▼
Move / Backfill data
   │
   ▼
Replicate data
   │
   ▼
Node becomes active
```

---

## 🛡️ Fault Tolerance

If a replica becomes unavailable:

```text
Node 1 → PRIMARY   ❌
Node 2 → REPLICA   ✅
Node 3 → REPLICA   ✅
```

ShardNest can continue operating using the available replicas, depending on the configured quorum requirements.

For writes:

```text
Node 1 → FAILED
Node 2 → ACK
Node 3 → ACK

W = 2

Write succeeds ✅
```

For reads, the system can fall back to available replicas when the preferred node is unavailable.

---

## 🔧 Backfill & Reconciliation

Replication can become incomplete because of:

* Node failures
* Network interruptions
* Temporary database failures
* Node additions
* Node removals
* Rebalancing

ShardNest provides a reconciliation/backfill mechanism to identify data that is missing from replicas and restore the required replication factor.

```text
Expected:

RF = 3

Node 1 → Data ✅
Node 2 → Data ✅
Node 3 → Data ❌

          │
          ▼
    Reconciliation
          │
          ▼
Node 3 → Data restored ✅
```

---

## ⚡ Redis Caching

ShardNest uses Redis with a **cache-aside** strategy.

```text
                ┌─────────────┐
                │   Request   │
                └──────┬──────┘
                       │
                       ▼
                 ┌───────────┐
                 │   Redis   │
                 └─────┬─────┘
                       │
                ┌──────┴──────┐
                │             │
               HIT           MISS
                │             │
                ▼             ▼
             Return         Database
                              │
                              ▼
                         Redis SET
                              │
                              ▼
                           Return
```

### Cache Invalidation

ShardNest uses **delete-on-write**:

```text
UPDATE
  │
  ▼
Database
  │
  ▼
Replication / Quorum
  │
  ▼
Successful write
  │
  ▼
DELETE Redis key
```

The next read repopulates Redis from the database.

This keeps the database as the source of truth and reduces the complexity of keeping two copies synchronized.

---

## 🧱 Tech Stack

| Layer         | Technology                         |
| ------------- | ---------------------------------- |
| Language      | Java 23                            |
| Framework     | Spring Boot 4.1                    |
| ORM           | Hibernate / Spring Data JPA        |
| Database      | MySQL 9.2                          |
| Sharding      | Consistent Hashing + Virtual Nodes |
| Replication   | RF=3 + Quorum                      |
| Cache         | Redis 7                            |
| Redis Client  | Lettuce                            |
| Serialization | Jackson 3                          |
| Hash Function | Murmur3 (Guava)                    |
| ID Generation | ULID                               |
| Build Tool    | Maven                              |

---

## 📁 High-Level Project Structure

```text
src/
└── main/
    ├── java/
    │   └── com/
    │       └── shardnest/
    │           ├── config/
    │           ├── controller/
    │           ├── service/
    │           ├── repository/
    │           ├── entity/
    │           ├── shard/
    │           ├── replication/
    │           ├── cache/
    │           ├── rebalance/
    │           └── ShardNestApplication.java
    │
    └── resources/
        └── application.yml
```

---

## 🚀 Running ShardNest

### Prerequisites

Make sure the following are installed:

* Java 23
* Maven
* Docker
* MySQL
* Redis

### Start Redis

```bash
docker run -d --name redis-shardnest -p 6379:6379 redis:7-alpine
```

Verify Redis:

```bash
docker ps
```

Test Redis:

```bash
docker exec -it redis-shardnest redis-cli
```

Then:

```redis
PING
```

Expected:

```text
PONG
```

### Build the project

```bash
mvn clean install
```

### Run the application

```bash
mvn spring-boot:run
```

---

## ⚙️ Example Replication Configuration

```yaml
shard:
  replication:
    factor: 3
    write-quorum: 2
    read-quorum: 2
```

Where:

| Configuration  | Meaning                                                          |
| -------------- | ---------------------------------------------------------------- |
| `factor`       | Number of replicas maintained                                    |
| `write-quorum` | Minimum replica acknowledgements required for a successful write |
| `read-quorum`  | Minimum replica responses required for a quorum read             |

---

## 🔑 Core Concepts

ShardNest combines several distributed-system concepts:

```text
                 ┌─────────────────────┐
                 │     ShardNest       │
                 └──────────┬──────────┘
                            │
                   Consistent Hashing
                            │
                            ▼
                     Virtual Nodes
                            │
                            ▼
                         Sharding
                            │
                            ▼
                       Replication
                            │
                            ▼
                     Quorum Consensus
                            │
                            ▼
                       Fault Tolerance
                            │
                            ▼
                     Rebalancing
                            │
                            ▼
                         Redis
                            │
                            ▼
                       Observability
                            │
                            ▼
                       Load Testing
```

---

## 📈 Future Improvements

* [ ] Automatic failure detection
* [ ] Leader election
* [ ] Automatic replica promotion
* [ ] Read repair
* [ ] Hinted handoff
* [ ] Write-ahead logging
* [ ] Persistent node metadata
* [ ] Prometheus metrics
* [ ] Grafana dashboards
* [ ] Distributed tracing
* [ ] Docker Compose cluster
* [ ] Kubernetes deployment
* [ ] Automated load testing
* [ ] Stronger consistency/versioning mechanisms
* [ ] Cluster health monitoring
* [ ] Automated data repair

---

## 📚 Design Inspiration

ShardNest is inspired by concepts described in:

* **Amazon Dynamo — 2007**
  Consistent hashing, replication, and quorum-based distributed storage.

* **Apache Cassandra**
  Virtual nodes, replication, partitioning, and tunable consistency.

* **MongoDB**
  Replica sets and distributed database architecture.

* **Designing Data-Intensive Applications** by Martin Kleppmann
  Distributed systems, replication, partitioning, consistency, and fault tolerance.

---

## 🎯 Learning Goals

The primary goal of ShardNest is to understand how distributed databases work internally rather than relying only on managed database features.

The project focuses on:

```text
                    Distributed Storage
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
      Partitioning     Replication      Caching
          │                │                │
          ▼                ▼                ▼
     Consistent         Quorum           Redis
      Hashing           Reads/Writes
          │                │
          └────────┬───────┘
                   ▼
             Fault Tolerance
                   │
                   ▼
             Rebalancing
```

---

## 👨‍💻 Author

**Bhuvan V**

📧 Email: [bhuvanvachar0123@gmail.com](mailto:bhuvanvachar0123@gmail.com)

🔗 LinkedIn: https://www.linkedin.com/in/bhuvan-v-188284246

---

## ⭐ If You Find This Project Useful

Feel free to explore the code, experiment with different replication factors and quorum configurations, and use the project as a learning resource for distributed systems and backend engineering.
