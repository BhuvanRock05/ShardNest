package com.shardNest.shard;

import java.util.List;

public interface ShardRouter {
    Shard getShard(String key);

    List<Shard> getAllShards();

    /**
     * Return the replication set for the given key.
     *
     * CONTRACT:
     *   - First element is the PRIMARY shard.
     *   - Subsequent elements are REPLICAS.
     *   - Elements are ordered clockwise on the ring.
     *   - All elements are distinct shards.
     *   - If the ring has fewer shards than `rf`, returns what exists.
     *
     * @param key the key to route
     * @param rf  replication factor (>= 1)
     * @return    ordered list of shards, primary first
     */
    List<Shard> getReplicas(String key, int rf);

    /**
     * Return the replication set using the configured default RF.
     */
    List<Shard> getReplicas(String key);
}
