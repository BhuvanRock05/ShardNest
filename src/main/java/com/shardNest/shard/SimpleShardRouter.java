package com.shardNest.shard;

import java.util.List;

public class SimpleShardRouter implements ShardRouter {

    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    private final List<Shard> shards;
    private final int replicationFactor;

    public SimpleShardRouter(List<Shard> shards) {
        this(shards, DEFAULT_REPLICATION_FACTOR);
    }

    public SimpleShardRouter(List<Shard> shards, int replicationFactor) {
        this.shards = shards;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Shard getShard(String key) {
        int index = Math.floorMod(key.hashCode(), shards.size());
        return shards.get(index);
    }

    @Override
    public List<Shard> getAllShards() {
        return List.copyOf(shards);
    }

    @Override
    public List<Shard> getReplicas(String key, int rf) {
        int effectiveRf = Math.min(rf, shards.size());
        int primaryIndex = Math.floorMod(key.hashCode(), shards.size());

        // Walk around the list, collecting distinct shards
        java.util.List<Shard> result = new java.util.ArrayList<>(effectiveRf);
        for (int i = 0; i < effectiveRf; i++) {
            result.add(shards.get((primaryIndex + i) % shards.size()));
        }
        return result;
    }

    @Override
    public List<Shard> getReplicas(String key) {
        return getReplicas(key, replicationFactor);
    }
}