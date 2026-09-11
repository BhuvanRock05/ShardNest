package com.shardNest.config;

import com.shardNest.shard.ConsistentHashRouter;
import com.shardNest.shard.Shard;
import com.shardNest.shard.ShardRouter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

@Configuration
public class ShardConfig {

    @Bean
    public List<Shard> shards() {
        return List.of(
                new Shard("shard1", "userdb1"),  // Must match DataSourceConfig keys
                new Shard("shard2", "userdb2"),
                new Shard("shard3", "userdb3")
        );
    }

    @Bean
    @Primary  // ← Both ShardRouter AND ConsistentHashRouter resolve here
    public ConsistentHashRouter consistentHashRouter(List<Shard> shards) {
        return new ConsistentHashRouter(shards, 100);
    }
}
