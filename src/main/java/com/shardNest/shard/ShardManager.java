package com.shardNest.shard;

import com.shardNest.config.ShardContext;
import com.shardNest.model.Address;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ShardManager {

    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);
    private static final int PAGE_SIZE = 500;

    private final ConsistentHashRouter router;
    private final UserRepository userRepository;
    private final AtomicBoolean rebalancing = new AtomicBoolean(false);

    public ShardManager(ConsistentHashRouter router, UserRepository userRepository) {
        this.router = router;
        this.userRepository = userRepository;
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
            List<User> page = readPage(source, lastId);
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
            List<User> page = readPage(source, lastId);
            if (page.isEmpty()) break;

            for (User user : page) {
                scanned++;
                // Ring no longer contains `source`, so this returns the new target
                Shard target = router.getShard(user.getUserId());
                moveUser(user, source, target);
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

    // ═══════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════
    private List<User> readPage(Shard shard, String lastId) {
        ShardContext.setShard(shard.getShardId());
        try {
            if (lastId == null) {
                return userRepository.findAll(
                        PageRequest.of(0, PAGE_SIZE, Sort.by("userId"))
                ).getContent();
            }
            return userRepository.findNextPage(lastId, PageRequest.of(0, PAGE_SIZE));
        } finally {
            ShardContext.clear();
        }
    }

    private void moveUser(User user, Shard from, Shard to) {

        User copyUser = copyForTransfer(user);

        // 1. Write to target
        ShardContext.setShard(to.getShardId());
        try {
            userRepository.saveAndFlush(copyUser);
        } finally {
            ShardContext.clear();
        }

        // 2. Verify write
        ShardContext.setShard(to.getShardId());
        try {
            if (!userRepository.existsById(copyUser.getUserId())) {
                throw new IllegalStateException("Verify failed for " + copyUser.getUserId());
            }
        } finally {
            ShardContext.clear();
        }

        // 3. Delete from source
        ShardContext.setShard(from.getShardId());
        try {
            userRepository.deleteById(user.getUserId());
        } finally {
            ShardContext.clear();
        }
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

    // ═══════════════════════════════════════════════
    // RESULT TYPES
    // ═══════════════════════════════════════════════
    public record ShardAddResult(Shard shard, long moved) {}
    public record ShardRemoveResult(Shard shard, long moved) {}
}