package com.shardNest.config;

import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties(prefix = "shard.replication")
@Validated
@Getter
@Setter
public class ReplicationConfig {

    @Min(1)
    private int factor = 3;

    @Min(1)
    private int writeQuorum = 2;      // W — used in Phase 7b

    @Min(1)
    private int readQuorum = 2;       // R — used in Phase 7b

    // getters/setters
}