package com.shardNest.shard;

import java.util.List;
import java.util.UUID;

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
}
