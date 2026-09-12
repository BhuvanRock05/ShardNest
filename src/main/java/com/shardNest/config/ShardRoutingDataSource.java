package com.shardNest.config;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShardRoutingDataSource extends AbstractRoutingDataSource {

    private final Map<Object, Object> mutableTargetMap = new ConcurrentHashMap<>();

    @Override
    public void setTargetDataSources(Map<Object, Object> targetDataSources) {
        mutableTargetMap.clear();
        mutableTargetMap.putAll(targetDataSources);
        super.setTargetDataSources(mutableTargetMap);
    }

    @Override
    protected @Nullable Object determineCurrentLookupKey() {
        Object shard = ShardContext.getShard();
        System.out.println(">>> ROUTING to shard: " + shard
                + " [thread=" + Thread.currentThread().getName() + "]");
        return shard;
    }

    public void addShardDataSource(String shardId, DataSource dataSource) {
        mutableTargetMap.put(shardId, dataSource);
        afterPropertiesSet();
    }

    public void removeShardDataSource(String shardId) {
        mutableTargetMap.remove(shardId);
        afterPropertiesSet();
    }
}