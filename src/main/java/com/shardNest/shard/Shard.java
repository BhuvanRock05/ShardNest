package com.shardNest.shard;

import lombok.Data;

@Data
public class Shard {

    private final String shardId;
    private final String databaseName;

    public Shard(String shardId, String databaseName) {
        this.shardId = shardId;
        this.databaseName = databaseName;
    }

}
