package com.shardNest.controller;

import com.shardNest.dto.ShardProvisionRequest;
import com.shardNest.shard.databases.DataSourceProvisioningService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/admin/datasource")
public class DataSourceAdminController {

    private final DataSourceProvisioningService provisioningService;

    public DataSourceAdminController(DataSourceProvisioningService provisioningService) {
        this.provisioningService = provisioningService;
    }

    /**
     * STEP 1: Provision a new database + DataSource.
     * Does NOT add to the hash ring yet.
     */
    @PostMapping("/provision")
    public ResponseEntity<?> provision(@RequestBody ShardProvisionRequest req) {
        try {
            DataSourceProvisioningService.ProvisionResult result = provisioningService.provision(req);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error",
                    e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }
}