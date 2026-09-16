package com.shardNest.shard;

import java.util.List;

public interface ShardRouter {
    Shard getShard(String key);

    List<Shard> getAllShards();
}
