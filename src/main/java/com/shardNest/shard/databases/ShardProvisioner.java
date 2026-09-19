package com.shardNest.shard.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Provisions a new shard: creates the database, creates tables,
 * and builds a ready-to-use DataSource.
 */
@Service
public class ShardProvisioner {

    private static final Logger log = LoggerFactory.getLogger(ShardProvisioner.class);

    @Value("${shard.provision.host:localhost}")
    private String defaultHost;

    @Value("${shard.provision.port:3306}")
    private int defaultPort;

    @Value("${shard.provision.admin-username:root}")
    private String adminUsername;

    @Value("${shard.provision.admin-password:root@123}")
    private String adminPassword;

    @Value("${shard.provision.max-pool-size:10}")
    private int maxPoolSize;

    @Value("${shard.provision.min-idle:2}")
    private int minIdle;

    /**
     * Creates the database + tables, then returns a DataSource.
     */
    public DataSource provision(String databaseName) {
        return provision(databaseName, defaultHost, defaultPort, adminUsername, adminPassword);
    }

    public DataSource provision(String databaseName,
                                String host,
                                int port,
                                String username,
                                String password) {

        validateDatabaseName(databaseName);

        log.info("═══ Provisioning database {} on {}:{} ═══", databaseName, host, port);

        // 1. Create the database
        createDatabase(host, port, username, password, databaseName);

        // 2. Create tables
        createTables(host, port, username, password, databaseName);

        // 3. Build the HikariDataSource for this new database
        DataSource ds = buildDataSource(host, port, username, password, databaseName);

        log.info("═══ Database {} provisioned successfully ═══", databaseName);
        return ds;
    }

    // ═══════════════════════════════════════════════
    // 1. CREATE DATABASE
    // ═══════════════════════════════════════════════
    private void createDatabase(String host, int port, String username,
                                String password, String databaseName) {
        String adminUrl = "jdbc:mysql://" + host + ":" + port +
                "/?useSSL=false&allowPublicKeyRetrieval=true";
        String sql = "CREATE DATABASE IF NOT EXISTS " + databaseName +
                " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";

        try (Connection conn = DriverManager.getConnection(adminUrl, username, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            log.info("  ✓ Database {} ready", databaseName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create database " + databaseName, e);
        }
    }

    // ═══════════════════════════════════════════════
    // 2. CREATE TABLES
    // ═══════════════════════════════════════════════
    private void createTables(String host, int port, String username,
                              String password, String databaseName) {
        String dbUrl = "jdbc:mysql://" + host + ":" + port + "/" + databaseName +
                "?useSSL=false&allowPublicKeyRetrieval=true";

        String addressTable = """
            CREATE TABLE IF NOT EXISTS address_table (
                id BIGINT NOT NULL AUTO_INCREMENT,
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(255),
                country VARCHAR(255),
                zip_code VARCHAR(255),
                PRIMARY KEY (id)
            ) ENGINE=InnoDB
            """;

        String usersTable = """
            CREATE TABLE IF NOT EXISTS users_table (
                user_id VARCHAR(255) NOT NULL,
                address_id BIGINT,
                user_name VARCHAR(255),
                password VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(255),
                PRIMARY KEY (user_id),
                UNIQUE KEY UK_address (address_id),
                CONSTRAINT FK_address FOREIGN KEY (address_id)
                    REFERENCES address_table(id)
            ) ENGINE=InnoDB
            """;

        try (Connection conn = DriverManager.getConnection(dbUrl, username, password);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(addressTable);
            stmt.executeUpdate(usersTable);
            log.info("  ✓ Tables created in {}", databaseName);

        } catch (Exception e) {
            throw new RuntimeException("Failed to create tables in " + databaseName, e);
        }
    }

    // ═══════════════════════════════════════════════
    // 3. BUILD DATASOURCE
    // ═══════════════════════════════════════════════
    private DataSource buildDataSource(String host, int port, String username,
                                       String password, String databaseName) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + databaseName +
                "?useSSL=false&allowPublicKeyRetrieval=true");
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minIdle);
        config.setPoolName("shard-" + databaseName);

        return new HikariDataSource(config);
    }

    // ═══════════════════════════════════════════════
    // VALIDATION
    // ═══════════════════════════════════════════════
    private void validateDatabaseName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Database name is required");
        }
        // Allow only alphanumeric + underscore, max 64 chars
        if (!name.matches("^[a-zA-Z][a-zA-Z0-9_]{0,63}$")) {
            throw new IllegalArgumentException(
                    "Invalid database name: must start with letter, " +
                            "contain only alphanumeric and _, max 64 chars"
            );
        }
    }
}