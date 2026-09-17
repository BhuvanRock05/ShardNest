package com.shardNest.shard;

import com.shardNest.config.ShardRoutingDataSource;
import com.shardNest.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
public class ShardManager {

    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);

    private final ConsistentHashRouter router;
    private final ShardOperationService shardOp;
    private final ShardRoutingDataSource routingDataSource;
    private final AtomicBoolean rebalancing = new AtomicBoolean(false);

    public ShardManager(ConsistentHashRouter router,
                        ShardOperationService shardOp,
                        ShardRoutingDataSource routingDataSource) {
        this.router = router;
        this.shardOp = shardOp;
        this.routingDataSource = routingDataSource;
    }

    // ═══════════════════════════════════════════════
    // ADD SHARD
    // ═══════════════════════════════════════════════
    public ShardAddResult addShard(Shard newShard) {
        if (!routingDataSource.hasShard(newShard.getShardId())) {
            throw new IllegalStateException(
                    "DataSource for shard '" + newShard.getShardId() + "' is not registered. "
                            + "Call POST /api/admin/datasource/provision first."
            );
        }

        boolean alreadyInRing = router.getAllShards().stream()
                .anyMatch(s -> s.getShardId().equals(newShard.getShardId()));
        if (alreadyInRing) {
            throw new IllegalStateException(
                    "Shard '" + newShard.getShardId() + "' is already in the ring"
            );
        }

        acquireLock();
        try {
            ConsistentHashRouter oldRouter = router.snapshot();
            log.info("═══ Adding shard {} to ring ═══", newShard.getShardId());

            router.addShard(newShard);

            long totalAffected = rebalanceReplicaSets(oldRouter);

            log.info("═══ Done. Adjusted replicas for {} users ═══", totalAffected);
            return new ShardAddResult(newShard, totalAffected);
        } finally {
            releaseLock();
        }
    }

    // ═══════════════════════════════════════════════
    // REMOVE SHARD
    // ═══════════════════════════════════════════════
    public ShardRemoveResult removeShard(Shard shard) {
        acquireLock();
        try {
            ConsistentHashRouter oldRouter = router.snapshot();
            log.info("═══ Removing shard {} from ring ═══", shard.getShardId());

            router.removeShard(shard);

            long totalAffected = rebalanceReplicaSets(oldRouter);

            long remaining = shardOp.countUsersInShard(shard);
            if (remaining > 0) {
                log.warn("⚠ Shard {} still has {} users after drain", shard.getShardId(), remaining);
            } else {
                log.info("✓ Shard {} is empty", shard.getShardId());
            }

            log.info("═══ Done. Adjusted replicas for {} users ═══", totalAffected);
            return new ShardRemoveResult(shard, totalAffected);
        } finally {
            releaseLock();
        }
    }

    // ═══════════════════════════════════════════════
    // CORE REBALANCE LOGIC
    // ═══════════════════════════════════════════════
    private long rebalanceReplicaSets(ConsistentHashRouter oldRouter) {
        List<Shard> shardsToScan = oldRouter.getAllShards();
        long affected = 0;

        for (Shard source : shardsToScan) {
            affected += processShardForRebalance(source, oldRouter);
        }
        return affected;
    }

    private long processShardForRebalance(Shard source, ConsistentHashRouter oldRouter) {
        long scanned = 0;
        long fixed = 0;
        String lastId = null;

        while (true) {
            List<User> page = shardOp.readPage(source, lastId);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                if (fixUserReplicas(user, oldRouter)) {
                    fixed++;
                }
            }
            lastId = page.get(page.size() - 1).getUserId();

            if (scanned % 1000 == 0) {
                log.info("  Rebalancing: scanned={}, fixed={}", scanned, fixed);
            }
        }

        log.info("  DONE scanning {}. scanned={}, fixed={}",
                source.getShardId(), scanned, fixed);
        return fixed;
    }

    /**
     * Fix replica placement for a single user.
     * Returns true if the user's replica set changed.
     */
    private boolean fixUserReplicas(User user, ConsistentHashRouter oldRouter) {
        String userId = user.getUserId();

        List<Shard> oldReplicas = oldRouter.getReplicas(userId);
        List<Shard> newReplicas = router.getReplicas(userId);

        Set<String> oldIds = oldReplicas.stream()
                .map(Shard::getShardId)
                .collect(Collectors.toSet());
        Set<String> newIds = newReplicas.stream()
                .map(Shard::getShardId)
                .collect(Collectors.toSet());

        if (oldIds.equals(newIds)) {
            return false;
        }

        Set<String> toAdd = new HashSet<>(newIds);
        toAdd.removeAll(oldIds);

        Set<String> toRemove = new HashSet<>(oldIds);
        toRemove.removeAll(newIds);

        log.debug("Fixing user {}: +{}, -{}", userId, toAdd, toRemove);

        for (String shardId : toAdd) {
            try {
                shardOp.writeUserToShard(user, shardId);
            } catch (Exception e) {
                log.warn("Failed to add user {} to {}: {}", userId, shardId, e.getMessage());
            }
        }

        for (String shardId : toRemove) {
            try {
                shardOp.deleteUserFromShard(userId, shardId);
            } catch (Exception e) {
                log.warn("Failed to remove user {} from {}: {}", userId, shardId, e.getMessage());
            }
        }

        return true;
    }

    // ═══════════════════════════════════════════════
    // BACKFILL — reconcile replica counts for all users
    // ═══════════════════════════════════════════════
    public BackfillResult backfillReplicas() {
        acquireLock();
        try {
            log.info("═══ Starting backfill ═══");
            Set<String> seenUsers = new HashSet<>();
            long scanned = 0;
            long fixed = 0;
            long skipped = 0;

            for (Shard shard : router.getAllShards()) {
                log.info("  Scanning shard {}...", shard.getShardId());
                long[] result = backfillShard(shard, seenUsers);
                scanned += result[0];
                fixed += result[1];
                skipped += result[2];
            }

            log.info("═══ Backfill done. scanned={}, skipped={}, fixed={} ═══",
                    scanned, skipped, fixed);
            return new BackfillResult(scanned, fixed);
        } finally {
            releaseLock();
        }
    }

    private long[] backfillShard(Shard source, Set<String> seenUsers) {
        long scanned = 0;
        long fixed = 0;
        long skipped = 0;
        String lastId = null;

        while (true) {
            List<User> page;
            try {
                page = shardOp.readPage(source, lastId);
            } catch (Exception e) {
                log.warn("Backfill: failed to read page from {}: {}",
                        source.getShardId(), e.getMessage());
                break;   // skip this shard, move to next
            }
            if (page.isEmpty()) break;

            for (User user : page) {
                if (!seenUsers.add(user.getUserId())) {
                    skipped++;
                    continue;   // already processed this user from another shard
                }
                scanned++;
                if (backfillUser(user)) {
                    fixed++;
                }
            }
            lastId = page.get(page.size() - 1).getUserId();

            if (scanned % 1000 == 0) {
                log.info("  Backfill: scanned={}, fixed={}", scanned, fixed);
            }
        }
        log.info("  Shard {} scanned: {}, fixed: {}", source.getShardId(), scanned, fixed);
        return new long[]{scanned, fixed, skipped};
    }

    /**
     * Ensure a user exists on all expected replicas.
     * Adds missing copies; never removes.
     * Returns true if any replica was added.
     */
    private boolean backfillUser(User user) {
        String userId = user.getUserId();
        List<Shard> expected = router.getReplicas(userId);

        boolean changed = false;

        for (Shard shard : expected) {
            boolean exists;
            try {
                exists = shardOp.existsOnShard(userId, shard.getShardId());
            } catch (Exception e) {
                log.warn("existsOnShard failed for {} on {}: {}",
                        userId, shard.getShardId(), e.getMessage());
                exists = false;   // treat as missing
            }

            if (exists) {
                continue;   // already there
            }

            try {
                shardOp.writeUserToShard(user, shard.getShardId());
                log.debug("Backfill: added {} to {}", userId, shard.getShardId());
                changed = true;
            } catch (Exception e) {
                log.warn("Backfill failed for {} on {}: {}",
                        userId, shard.getShardId(), e.getMessage());
            }
        }

        return changed;
    }

    // ═══════════════════════════════════════════════
    // LOCK
    // ═══════════════════════════════════════════════
    private void acquireLock() {
        if (!rebalancing.compareAndSet(false, true)) {
            throw new IllegalStateException("Another rebalance is already in progress");
        }
    }

    private void releaseLock() {
        rebalancing.set(false);
    }

    public boolean isRebalancing() {
        return rebalancing.get();
    }

    // ═══════════════════════════════════════════════
    // RESULT TYPES
    // ═══════════════════════════════════════════════
    public record ShardAddResult(Shard shard, long moved) {}
    public record ShardRemoveResult(Shard shard, long moved) {}
    public record BackfillResult(long scanned, long fixed) {}
}