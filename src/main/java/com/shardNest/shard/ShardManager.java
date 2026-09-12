package com.shardNest.shard;

import com.shardNest.model.Address;
import com.shardNest.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ShardManager {

    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);
    private static final int PAGE_SIZE = 500;

    private final ConsistentHashRouter router;
    private final ShardOperationService shardOp;
    private final AtomicBoolean rebalancing = new AtomicBoolean(false);

    public ShardManager(ConsistentHashRouter router, ShardOperationService shardOp) {
        this.router = router;
        this.shardOp = shardOp;
    }

    // ═══════════════════════════════════════════════
    // ADD SHARD
    // ═══════════════════════════════════════════════
    public ShardAddResult addShard(Shard newShard) {
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
        List<Shard> sources = router.getAllShards().stream()
                .filter(s -> !s.getShardId().equals(newShard.getShardId()))
                .toList();
        for (Shard source : sources) {
            totalMoved += moveKeysTargeting(source, newShard);
        }
        return totalMoved;
    }

    private long moveKeysTargeting(Shard source, Shard target) {
        long scanned = 0;
        long moved = 0;
        String lastId = null;

        while (true) {
            List<User> page = shardOp.readPageFromShard(source.getShardId(), lastId, PAGE_SIZE);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                Shard currentTarget = router.getShard(user.getUserId());
                if (currentTarget.getShardId().equals(target.getShardId())) {
                    moveUser(user, source, target);
                    moved++;
                }
            }
            lastId = page.get(page.size() - 1).getUserId();
        }
        log.info("  {} → {}: DONE. scanned={}, moved={}",
                source.getShardId(), target.getShardId(), scanned, moved);
        return moved;
    }

    // ═══════════════════════════════════════════════
    // REMOVE SHARD
    // ═══════════════════════════════════════════════
    public ShardRemoveResult removeShard(Shard shard) {
        acquireLock();
        try {
            log.info("═══ Removing shard {} from ring ═══", shard.getShardId());
            router.removeShard(shard);
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
            List<User> page = shardOp.readPageFromShard(source.getShardId(), lastId, PAGE_SIZE);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                Shard target = router.getShard(user.getUserId());
                moveUser(user, source, target);
                moved++;
            }
            lastId = page.get(page.size() - 1).getUserId();
        }
        log.info("  Draining {}: DONE. scanned={}, moved={}",
                source.getShardId(), scanned, moved);
        return moved;
    }

    // ═══════════════════════════════════════════════
    // MOVE ONE USER
    // ═══════════════════════════════════════════════
    private void moveUser(User user, Shard from, Shard to) {
        User copy = copyForTransfer(user);

        // 1. INSERT to target — uses fresh transaction + fresh EntityManager
        shardOp.saveToShard(to.getShardId(), copy);

        // 2. Verify
        if (!shardOp.existsInShard(to.getShardId(), copy.getUserId())) {
            throw new IllegalStateException("Verify failed for " + copy.getUserId());
        }

        // 3. Delete from source
        shardOp.deleteFromShard(from.getShardId(), copy.getUserId());
    }

    private User copyForTransfer(User source) {
        User copy = new User();
        copy.setUserId(source.getUserId());
        copy.setUserName(source.getUserName());
        copy.setPassword(source.getPassword());
        copy.setFirstName(source.getFirstName());
        copy.setLastName(source.getLastName());
        copy.setEmail(source.getEmail());
        copy.setPhone(source.getPhone());

        if (source.getAddress() != null) {
            Address addr = new Address();
            // id = null → target DB generates a fresh address_id
            addr.setStreet(source.getAddress().getStreet());
            addr.setCity(source.getAddress().getCity());
            addr.setState(source.getAddress().getState());
            addr.setCountry(source.getAddress().getCountry());
            addr.setZipCode(source.getAddress().getZipCode());
            copy.setAddress(addr);
        }
        return copy;
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

    public record ShardAddResult(Shard shard, long moved) {}
    public record ShardRemoveResult(Shard shard, long moved) {}
}