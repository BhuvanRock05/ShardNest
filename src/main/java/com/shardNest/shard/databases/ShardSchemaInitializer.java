package com.shardNest.shard.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Component
public class ShardSchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(ShardSchemaInitializer.class);

    /**
     * CREATE DATABASE IF NOT EXISTS <db>.
     * Connects to server-level URL (no database in URL).
     */
    public void createDatabase(String jdbcUrl,
                               String username,
                               String password,
                               String driverClassName,
                               String databaseName) throws Exception {

        // Strip trailing /db → server-level URL
        int lastSlash = jdbcUrl.lastIndexOf('/');
        String serverUrl = jdbcUrl.substring(0, lastSlash + 1);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(serverUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);
        config.setMaximumPoolSize(1);
        config.setPoolName("HikariPool-ddl-" + databaseName);

        try (HikariDataSource ds = new HikariDataSource(config);
             Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE DATABASE IF NOT EXISTS " + databaseName
                    + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
            log.info("✓ Database '{}' ready", databaseName);
        }
    }

    /**
     * CREATE TABLE IF NOT EXISTS for users_table and address_table.
     */
    public void createTables(DataSource dataSource) throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS address_table (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    city VARCHAR(255),
                    country VARCHAR(255),
                    state VARCHAR(255),
                    street VARCHAR(255),
                    zip_code VARCHAR(255),
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS users_table (
                    user_id VARCHAR(255) NOT NULL,
                    address_id BIGINT,
                    email VARCHAR(255),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    password VARCHAR(255),
                    phone VARCHAR(255),
                    user_name VARCHAR(255),
                    PRIMARY KEY (user_id),
                    UNIQUE KEY UK_address (address_id),
                    CONSTRAINT FK_address FOREIGN KEY (address_id)
                        REFERENCES address_table(id)
                ) ENGINE=InnoDB
            """);

            log.info("✓ Tables created in target database");
        }
    }
}