package com.shardNest.config;

import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class ShardRoutingDataSource extends AbstractRoutingDataSource {


    @Override
    protected @Nullable Object determineCurrentLookupKey() {
        return ShardContext.getShard();
    }
}
