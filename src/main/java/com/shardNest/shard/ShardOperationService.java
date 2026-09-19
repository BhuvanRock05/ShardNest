package com.shardNest.shard;

import com.shardNest.config.ReplicationConfig;
import com.shardNest.config.ShardContext;
import com.shardNest.model.Address;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ShardOperationService {

    private static final Logger log = LoggerFactory.getLogger(ShardOperationService.class);
    private static final int PAGE_SIZE = 500;
    private static final long QUORUM_TIMEOUT_SECONDS = 5;

    private final UserRepository userRepository;
    private final int writeQuorum;
    private final ExecutorService replicaWritePool;

    public ShardOperationService(UserRepository userRepository,
                                 ReplicationConfig replicationConfig) {
        this.userRepository = userRepository;
        this.writeQuorum = replicationConfig.getWriteQuorum();
        this.replicaWritePool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "replica-writer");
            t.setDaemon(true);
            return t;
        });
    }

    // ═══════════════════════════════════════════════
    // QUORUM WRITES (NEW — Step 8)
    // ═══════════════════════════════════════════════

    /**
     * Write to all replicas IN PARALLEL.
     * Succeed as soon as `writeQuorum` replicas confirm.
     * The remaining replicas continue in the background.
     *
     * Throws if quorum is not reached within timeout.
     */
    public WriteResult writeToReplicasWithQuorum(List<Shard> replicas, User user) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("Replicas list cannot be empty");
        }

        int total = replicas.size();
        int effectiveQuorum = Math.min(writeQuorum, total);

        List<String> succeededShards = new CopyOnWriteArrayList<>();
        List<String> failedShardIds = new CopyOnWriteArrayList<>();
        CountDownLatch quorumLatch = new CountDownLatch(effectiveQuorum);

        long start = System.currentTimeMillis();

        // Fire all writes in parallel
        for (Shard shard : replicas) {
            replicaWritePool.submit(() -> {
                try {
                    writeOneReplica(shard, user);
                    succeededShards.add(shard.getShardId());
                    quorumLatch.countDown();
                    log.debug("✓ Wrote user {} to {}", user.getUserId(), shard.getShardId());
                } catch (Exception e) {
                    failedShardIds.add(shard.getShardId());
                    log.warn("✗ Write to {} failed for {}: {}",
                            shard.getShardId(), user.getUserId(), e.getMessage());
                }
            });
        }

        // Wait for quorum
        boolean quorumReached;
        try {
            quorumReached = quorumLatch.await(QUORUM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            quorumReached = false;
        }

        long elapsed = System.currentTimeMillis() - start;

        if (!quorumReached) {
            throw new IllegalStateException(
                    "Write quorum not reached for user " + user.getUserId()
                            + " within " + QUORUM_TIMEOUT_SECONDS + "s. "
                            + "Succeeded: " + succeededShards + ", "
                            + "Failed: " + failedShardIds);
        }

        log.debug("✓ Quorum ({}/{}) reached for user {} in {}ms. Succeeded: {}, Failed: {}",
                succeededShards.size(), total, user.getUserId(),
                elapsed, succeededShards, failedShardIds);

        return new WriteResult(total, succeededShards.size(), failedShardIds);
    }

    // ═══════════════════════════════════════════════
    // SEQUENTIAL WRITE (kept for compatibility)
    // ═══════════════════════════════════════════════

    /**
     * Write a user to ALL replicas sequentially.
     *
     * Kept for backward compatibility. New code should use
     * writeToReplicasWithQuorum.
     */
    @Deprecated
    public WriteResult writeToReplicas(List<Shard> replicas, User user) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("Replicas list cannot be empty");
        }

        int succeeded = 0;
        List<String> failedShardIds = new ArrayList<>(replicas.size());

        for (Shard shard : replicas) {
            try {
                writeOneReplica(shard, user);
                succeeded++;
            } catch (Exception e) {
                failedShardIds.add(shard.getShardId());
                log.warn("✗ Failed to write user {} to {}: {}",
                        user.getUserId(), shard.getShardId(), e.getMessage());
            }
        }

        WriteResult result = new WriteResult(replicas.size(), succeeded, failedShardIds);

        if (succeeded == 0) {
            throw new IllegalStateException(
                    "Write FAILED on all " + replicas.size() + " replicas for user "
                            + user.getUserId() + ". Failed shards: " + failedShardIds);
        }

        return result;
    }

    private void writeOneReplica(Shard shard, User sourceUser) {
        User copy = copyForTransfer(sourceUser);

        ShardContext.setShard(shard.getShardId());
        try {
            userRepository.saveAndFlush(copy);
            if (!userRepository.existsById(copy.getUserId())) {
                throw new IllegalStateException("Verify failed for " + copy.getUserId());
            }
        } finally {
            ShardContext.clear();
        }
    }

    // ═══════════════════════════════════════════════
    // DELETE (unchanged)
    // ═══════════════════════════════════════════════

    public WriteResult deleteFromReplicas(List<Shard> replicas, String userId) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("Replicas list cannot be empty");
        }

        int succeeded = 0;
        List<String> failedShardIds = new ArrayList<>(replicas.size());

        for (Shard shard : replicas) {
            try {
                ShardContext.setShard(shard.getShardId());
                try {
                    if (userRepository.existsById(userId)) {
                        userRepository.deleteById(userId);
                    }
                    succeeded++;
                } finally {
                    ShardContext.clear();
                }
            } catch (Exception e) {
                failedShardIds.add(shard.getShardId());
                log.warn("✗ Failed to delete user {} from {}: {}",
                        userId, shard.getShardId(), e.getMessage());
            }
        }

        WriteResult result = new WriteResult(replicas.size(), succeeded, failedShardIds);

        if (!result.isFullyReplicated()) {
            log.warn("Partial deletion for user {}: {}/{} succeeded, failed on {}",
                    userId, succeeded, replicas.size(), failedShardIds);
        }

        return result;
    }

    // ═══════════════════════════════════════════════
    // SINGLE-SHARD OPERATIONS (used by ShardManager)
    // ═══════════════════════════════════════════════

    public void writeUserToShard(User user, String shardId) {
        User copy = copyForTransfer(user);
        ShardContext.setShard(shardId);
        try {
            userRepository.saveAndFlush(copy);
        } finally {
            ShardContext.clear();
        }
    }

    public void deleteUserFromShard(String userId, String shardId) {
        ShardContext.setShard(shardId);
        try {
            if (userRepository.existsById(userId)) {
                userRepository.deleteById(userId);
            }
        } finally {
            ShardContext.clear();
        }
    }

    public boolean existsOnShard(String userId, String shardId) {
        ShardContext.setShard(shardId);
        try {
            return userRepository.existsById(userId);
        } finally {
            ShardContext.clear();
        }
    }

    // ═══════════════════════════════════════════════
    // MIGRATION HELPERS
    // ═══════════════════════════════════════════════

    List<User> readPage(Shard shard, String lastId) {
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

    long countUsersInShard(Shard shard) {
        ShardContext.setShard(shard.getShardId());
        try {
            return userRepository.count();
        } finally {
            ShardContext.clear();
        }
    }

    // ═══════════════════════════════════════════════
    // COPY HELPER
    // ═══════════════════════════════════════════════

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
            addr.setStreet(source.getAddress().getStreet());
            addr.setCity(source.getAddress().getCity());
            addr.setState(source.getAddress().getState());
            addr.setCountry(source.getAddress().getCountry());
            addr.setZipCode(source.getAddress().getZipCode());
            copy.setAddress(addr);
        }
        return copy;
    }

    // ═══════════════════════════════════════════════
    // RESULT TYPE
    // ═══════════════════════════════════════════════

    public record WriteResult(
            int attempted,
            int succeeded,
            List<String> failedShardIds
    ) {
        public boolean isFullyReplicated() {
            return succeeded == attempted;
        }

        public boolean isQuorumMet(int quorum) {
            return succeeded >= quorum;
        }
    }
}