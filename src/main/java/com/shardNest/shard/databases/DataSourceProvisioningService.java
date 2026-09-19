package com.shardNest.shard.databases;

import com.shardNest.config.ShardRoutingDataSource;
import com.shardNest.dto.ShardProvisionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;

@Service
public class DataSourceProvisioningService {

    private static final Logger log = LoggerFactory.getLogger(DataSourceProvisioningService.class);

    private final DynamicDataSourceFactory dataSourceFactory;
    private final ShardSchemaInitializer schemaInitializer;
    private final ShardRoutingDataSource routingDataSource;

    public DataSourceProvisioningService(
            DynamicDataSourceFactory dataSourceFactory,
            ShardSchemaInitializer schemaInitializer,
            @Qualifier("routingDataSource") DataSource routingDataSource) {
        this.dataSourceFactory = dataSourceFactory;
        this.schemaInitializer = schemaInitializer;
        this.routingDataSource = (ShardRoutingDataSource) routingDataSource;
    }

    public ProvisionResult provision(ShardProvisionRequest req) throws Exception {

        log.info("═══ Provisioning DataSource for shard '{}' ═══", req.getShardId());
        validate(req);

        // 1. Create database (optional)
        if (req.isCreateDatabase()) {
            schemaInitializer.createDatabase(
                    req.getJdbcUrl(),
                    req.getUsername(),
                    req.getPassword(),
                    req.getDriverClassName(),
                    req.getDatabaseName());
        }

        // 2. Build DataSource
        DataSource ds = dataSourceFactory.create(
                req.getShardId(),
                req.getJdbcUrl(),
                req.getUsername(),
                req.getPassword(),
                req.getDriverClassName());

        // 3. Test connection
        try (Connection conn = ds.getConnection()) {
            if (!conn.isValid(5)) {
                throw new IllegalStateException("DataSource connection is not valid");
            }
        }

        // 4. Create tables (optional)
        if (req.isCreateTables()) {
            schemaInitializer.createTables(ds);
        }

        // 5. Register DataSource in routing layer (NOT in ring yet)
        routingDataSource.addShardDataSource(req.getShardId(), ds);

        log.info("═══ DataSource '{}' is READY. Call /api/admin/shard/add to add it to the ring ═══",
                req.getShardId());

        return new ProvisionResult(
                req.getShardId(),
                req.getDatabaseName(),
                "READY",
                "Call POST /api/admin/shard/add?shardId=" + req.getShardId()
                        + "&databaseName=" + req.getDatabaseName()
                        + " to add to ring"
        );
    }

    private void validate(ShardProvisionRequest req) {
        if (req.getShardId() == null || req.getShardId().isBlank())
            throw new IllegalArgumentException("shardId is required");

        if (req.getJdbcUrl() == null || !req.getJdbcUrl().startsWith("jdbc:"))
            throw new IllegalArgumentException("jdbcUrl must start with jdbc:");

        if (req.getUsername() == null)
            throw new IllegalArgumentException("username is required");

        if (req.isCreateDatabase() &&
                (req.getDatabaseName() == null || req.getDatabaseName().isBlank()))
            throw new IllegalArgumentException("databaseName is required when createDatabase=true");

        if (routingDataSource.hasShard(req.getShardId()))
            throw new IllegalStateException("DataSource for shard '"
                    + req.getShardId() + "' is already provisioned");
    }

    public record ProvisionResult(
            String shardId,
            String databaseName,
            String status,
            String nextStep
    ) {}
}