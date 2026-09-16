package com.shardNest.shard;

import com.shardNest.config.ShardRoutingDataSource;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ShardManager {

    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);

    private final ConsistentHashRouter router;
    private final UserRepository userRepository;
    private final ShardOperationService shardOp;
    private final ShardRoutingDataSource routingDataSource;
    private final AtomicBoolean rebalancing = new AtomicBoolean(false);

    public ShardManager(ConsistentHashRouter router, UserRepository userRepository, ShardOperationService shardOp, ShardRoutingDataSource routingDataSource) {
        this.router = router;
        this.userRepository = userRepository;
        this.shardOp = shardOp;
        this.routingDataSource = routingDataSource;
    }

    // ═══════════════════════════════════════════════
    // ADD SHARD
    // ═══════════════════════════════════════════════
    public ShardAddResult addShard(Shard newShard) {
        // ⭐ PRECONDITION: DataSource must already be provisioned
        if (!routingDataSource.hasShard(newShard.getShardId())) {
            throw new IllegalStateException(
                    "DataSource for shard '" + newShard.getShardId() + "' is not registered. "
                            + "Call POST /api/admin/datasource/provision first."
            );
        }

        // Check the shard isn't already in the ring
        boolean alreadyInRing = router.getAllShards().stream()
                .anyMatch(s -> s.getShardId().equals(newShard.getShardId()));
        if (alreadyInRing) {
            throw new IllegalStateException(
                    "Shard '" + newShard.getShardId() + "' is already in the ring"
            );
        }

        acquireLock();
        try {
            log.info("═══ Adding shard {} to ring ═══", newShard.getShardId());
            router.addShard(newShard);

            log.info("═══ Rebalancing after adding {} ═══", newShard.getShardId());
            long totalMoved = rebalanceAfterAdd(newShard);

            log.info("═══ Done. Moved {} users to {} ═══", totalMoved, newShard.getShardId());
            return new ShardAddResult(newShard, totalMoved);
        } finally {
            releaseLock();
        }
    }

    private long rebalanceAfterAdd(Shard newShard) {
        long totalMoved = 0;

        // Get all shards EXCEPT the new one
        List<Shard> sources = router.getAllShards().stream()
                .filter(s -> !s.getShardId().equals(newShard.getShardId()))
                .toList();

        for (Shard source : sources) {
            totalMoved += moveKeysTargeting(source, newShard);
        }
        return totalMoved;
    }

    /**
     * Scan `source`, move any user whose new ring target is `target`.
     */
    private long moveKeysTargeting(Shard source, Shard target) {
        long scanned = 0;
        long moved = 0;
        String lastId = null;

        while (true) {
            List<User> page = shardOp.readPage(source, lastId);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                Shard currentTarget = router.getShard(user.getUserId());
                if (currentTarget.getShardId().equals(target.getShardId())) {
                    shardOp.moveUser(user, source, target);
                    moved++;
                }
            }
            lastId = page.get(page.size() - 1).getUserId();

            if (scanned % 1000 == 0) {
                log.info("  {} → {}: scanned={}, moved={}", source.getShardId(), target.getShardId(), scanned, moved);
            }
        }
        log.info("  {} → {}: DONE. scanned={}, moved={}", source.getShardId(), target.getShardId(), scanned, moved);
        return moved;
    }

    // ═══════════════════════════════════════════════
    // REMOVE SHARD
    // ═══════════════════════════════════════════════
    public ShardRemoveResult removeShard(Shard shard) {
        acquireLock();
        try {
            log.info("═══ Removing shard {} from ring ═══", shard.getShardId());

            // 1. Remove from ring FIRST so new writes/reads don't route here
            router.removeShard(shard);

            // 2. Drain all data off the shard
            long moved = drainShard(shard);

            log.info("═══ Done. Moved {} users off {} ═══", moved, shard.getShardId());
            return new ShardRemoveResult(shard, moved);
        } finally {
            releaseLock();
        }
    }

    private long drainShard(Shard source) {
        long scanned = 0;
        long moved = 0;
        String lastId = null;

        while (true) {
            List<User> page = shardOp.readPage(source, lastId);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                // Ring no longer contains `source`, so this returns the new target
                Shard target = router.getShard(user.getUserId());
                shardOp.moveUser(user, source, target);
                moved++;
            }
            lastId = page.get(page.size() - 1).getUserId();

            if (scanned % 1000 == 0) {
                log.info("  Draining {}: scanned={}, moved={}", source.getShardId(), scanned, moved);
            }
        }
        log.info("  Draining {}: DONE. scanned={}, moved={}", source.getShardId(), scanned, moved);
        return moved;
    }

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
}