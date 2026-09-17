package com.shardNest.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConsistentHashRouter implements ShardRouter {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRouter.class);

    private static final int DEFAULT_VNODE_COUNT = 100;
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    private final HashRing ring;
    private final int vnodeCount;
    private final int replicationFactor;

    public ConsistentHashRouter(List<Shard> shards) {
        this(shards, DEFAULT_VNODE_COUNT, DEFAULT_REPLICATION_FACTOR);
    }

    public ConsistentHashRouter(List<Shard> shards, int vnodeCount) {
        this(shards, vnodeCount, DEFAULT_REPLICATION_FACTOR);
    }

    public ConsistentHashRouter(List<Shard> shards, int vnodeCount, int replicationFactor) {
        this.vnodeCount = vnodeCount;
        this.replicationFactor = replicationFactor;
        this.ring = new HashRing();
        for (Shard shard : shards) {
            ring.addShard(shard, vnodeCount);
        }
    }

    /**
     * Private constructor for creating a router with a pre-built ring.
     * Used by snapshot().
     */
    private ConsistentHashRouter(HashRing existingRing,
                                 int vnodeCount,
                                 int replicationFactor) {
        this.vnodeCount = vnodeCount;
        this.replicationFactor = replicationFactor;
        this.ring = existingRing;
    }

    /**
     * Return a new ConsistentHashRouter using a SNAPSHOT of the current ring.
     * The returned router is fully independent — modifying it does not affect
     * this one, and vice versa.
     */
    public ConsistentHashRouter snapshot() {
        return new ConsistentHashRouter(
                this.ring.copyOf(),
                this.vnodeCount,
                this.replicationFactor
        );
    }

    @Override
    public Shard getShard(String key) {
        return ring.getShard(key);
    }

    @Override
    public List<Shard> getAllShards() {
        return ring.getDistinctShards();
    }

    @Override
    public List<Shard> getReplicas(String key, int rf) {
        List<Shard> replicas = ring.getShards(key, rf);
        if (replicas.size() < rf) {
            log.warn("Requested RF={} for key '{}' but ring has only {} distinct shards",
                    rf, key, replicas.size());
        }
        return replicas;
    }

    @Override
    public List<Shard> getReplicas(String key) {
        return getReplicas(key, replicationFactor);
    }

    public HashRing getRing() {
        return ring;
    }

    public int getVnodeCount() {
        return vnodeCount;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public synchronized void addShard(Shard shard) {
        ring.addShard(shard, vnodeCount);
    }

    public synchronized void removeShard(Shard shard) {
        ring.removeShard(shard, vnodeCount);
    }
}