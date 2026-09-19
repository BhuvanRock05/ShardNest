package com.shardNest.controller;

import com.shardNest.shard.Shard;
import com.shardNest.shard.ShardManager;
import com.shardNest.shard.ShardRouter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/shard")
public class ShardAdminController {

    private final ShardManager shardManager;
    private final ShardRouter shardRouter;

    public ShardAdminController(ShardManager shardManager, ShardRouter shardRouter) {
        this.shardManager = shardManager;
        this.shardRouter = shardRouter;
    }

    /**
     * Add a new shard to the ring and rebalance.
     */
    @PostMapping("/add")
    public ResponseEntity<?> addShard(@RequestParam String shardId,
                                      @RequestParam String databaseName) {
        try {
            Shard shard = new Shard(shardId, databaseName);
            ShardManager.ShardAddResult result = shardManager.addShard(shard);
            return ResponseEntity.ok(Map.of(
                    "shard", result.shard().getShardId(),
                    "moved", result.moved(),
                    "status", "OK"
            ));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Remove a shard from the ring and drain its data.
     */
    @PostMapping("/remove")
    public ResponseEntity<?> removeShard(@RequestParam String shardId) {
        try {
            Shard shard = shardRouter.getAllShards().stream()
                    .filter(s -> s.getShardId().equals(shardId))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown shard: " + shardId));

            ShardManager.ShardRemoveResult result = shardManager.removeShard(shard);
            return ResponseEntity.ok(Map.of(
                    "shard", result.shard().getShardId(),
                    "moved", result.moved(),
                    "status", "OK"
            ));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * List current shards in the ring.
     */
    @GetMapping("/list")
    public List<Map<String, String>> listShards() {
        return shardRouter.getAllShards().stream()
                .map(s -> Map.of(
                        "shardId", s.getShardId(),
                        "databaseName", s.getDatabaseName()
                ))
                .toList();
    }

    /**
     * Check if rebalancing is in progress.
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "rebalancing", shardManager.isRebalancing(),
                "shards", shardRouter.getAllShards().size()
        );
    }

    @PostMapping("/backfill")
    public ResponseEntity<?> backfill() {
        try {
            ShardManager.BackfillResult result = shardManager.backfillReplicas();
            return ResponseEntity.ok(Map.of(
                    "scanned", result.scanned(),
                    "fixed", result.fixed(),
                    "status", "OK"
            ));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }
}
