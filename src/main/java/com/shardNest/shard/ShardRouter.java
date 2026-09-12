package com.shardNest.shard;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

public interface ShardRouter {
    Shard getShard(String key);

    List<Shard> getAllShards();
}
