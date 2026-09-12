package com.shardNest.shard;

import java.util.List;

public class ConsistentHashRouter implements ShardRouter {

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

    @Override
    public List<Shard> getAllShards() {
        return ring.getDistinctShards();
    }

    public HashRing getRing() {
        return ring;
    }

    public int getVnodeCount() {
        return vnodeCount;
    }

    public synchronized void addShard(Shard shard) {
        ring.addShard(shard, vnodeCount);
    }

    public synchronized void removeShard(Shard shard) {
        ring.removeShard(shard, vnodeCount);
    }
}