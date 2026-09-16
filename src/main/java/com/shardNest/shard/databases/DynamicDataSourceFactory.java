package com.shardNest.shard.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class DynamicDataSourceFactory {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataSourceFactory.class);

    public DataSource create(String shardId,
                             String jdbcUrl,
                             String username,
                             String password,
                             String driverClassName) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);

        // Pool sizing — tune per workload
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(10_000);
        config.setIdleTimeout(300_000);
        config.setMaxLifetime(1_800_000);

        // Unique pool name per shard (avoids JMX/log collisions)
        config.setPoolName("HikariPool-" + shardId);

        // Fail fast on bad connection
        config.setConnectionTestQuery("SELECT 1");

        HikariDataSource ds = new HikariDataSource(config);
        log.info("✓ Created DataSource for shard '{}' at {}", shardId, jdbcUrl);
        return ds;
    }
}