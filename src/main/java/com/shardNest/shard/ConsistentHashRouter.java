package com.shardNest.shard;


import java.util.List;

public class ConsistentHashRouter implements ShardRouter {

    /**
     * Default number of virtual nodes per shard.
     *
     * Rule of thumb:
     *  - 100 VNodes  → good distribution for small clusters
     *  - 200 VNodes  → very good distribution
     *  - 500+ VNodes → diminishing returns, more memory
     *
     * Start with 100 for learning.
     */
    private static final int DEFAULT_VNODE_COUNT = 100;

    private final HashRing ring;
    private final int vnodeCount;

    public ConsistentHashRouter(List<Shard> shards) {
        this(shards, DEFAULT_VNODE_COUNT);
    }

    public ConsistentHashRouter(List<Shard> shards, int vnodeCount) {
        this.vnodeCount = vnodeCount;
        this.ring = new HashRing();
        for (Shard shard : shards) {
            ring.addShard(shard, vnodeCount);
        }
    }

    @Override
    public Shard getShard(String key) {
        return ring.getShard(key);
    }

    public HashRing getRing() {
        return ring;
    }

    public int getVnodeCount() {
        return vnodeCount;
    }

    public void addShard(Shard shard) {
        ring.addShard(shard, vnodeCount);
    }

    public void removeShard(Shard shard) {
        ring.removeShard(shard, vnodeCount);
    }
}
