package com.shardNest.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DataSourceConfig {

    @Bean
    @ConfigurationProperties("spring.datasource.shard1")
    public DataSource shard1DataSource() {

        return DataSourceBuilder.create()
                .build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.shard2")
    public DataSource shard2DataSource() {

        return DataSourceBuilder.create()
                .build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.shard3")
    public DataSource shard3DataSource() {

        return DataSourceBuilder.create()
                .build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.shard4")
    public DataSource shard4DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    public DataSource routingDataSource(
            @Qualifier("shard1DataSource") DataSource shard1,
            @Qualifier("shard2DataSource") DataSource shard2,
            @Qualifier("shard3DataSource") DataSource shard3,
            @Qualifier("shard4DataSource") DataSource shard4
    ) {

        Map<Object, Object> targetDataSources =
                new HashMap<>();

        targetDataSources.put("shard1", shard1);
        targetDataSources.put("shard2", shard2);
        targetDataSources.put("shard3", shard3);
        targetDataSources.put("shard4", shard4);

        ShardRoutingDataSource routingDataSource = new ShardRoutingDataSource();
        routingDataSource.setTargetDataSources(targetDataSources);
        routingDataSource.setDefaultTargetDataSource(shard1);
        routingDataSource.afterPropertiesSet();

        return routingDataSource;
    }
}
