package com.shardNest.config;

import lombok.Data;

@Data
public class ShardContext {

    private static final ThreadLocal<String> CURRENT_SHARD =
            new ThreadLocal<>();

    public static void setShard(String shard) {
        CURRENT_SHARD.set(shard);
    }

    public static String getShard() {
        return CURRENT_SHARD.get();
    }

    public static void clear() {
        CURRENT_SHARD.remove();
    }
}
