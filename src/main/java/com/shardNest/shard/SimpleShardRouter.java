package com.shardNest.shard;

import java.util.List;

public class SimpleShardRouter implements ShardRouter{

    private final List<Shard> shards;
    public SimpleShardRouter(List<Shard> shards) {
        this.shards = shards;
    }


    @Override
    public Shard getShard(String key) {
        int index = Math.floorMod(
                key.hashCode(),
                shards.size()
        );
        return shards.get(index);
    }

    @Override
    public List<Shard> getAllShards() {
        return List.copyOf(shards);
    }
}
