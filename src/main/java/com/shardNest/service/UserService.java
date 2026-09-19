package com.shardNest.service;

import com.github.f4b6a3.ulid.UlidCreator;
import com.shardNest.cache.CacheService;
import com.shardNest.config.ShardContext;
import com.shardNest.dto.UserRequest;
import com.shardNest.dto.UserResponse;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import com.shardNest.shard.Shard;
import com.shardNest.shard.ShardOperationService;
import com.shardNest.shard.ShardRouter;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ShardRouter router;

    @Autowired
    private ShardOperationService shardOp;

    @Autowired
    private CacheService cacheService;

    // ═══════════════════════════════════════════════
    // CREATE — quorum write
    // ═══════════════════════════════════════════════
    public UserResponse addUser(UserRequest userRequest) {
        User user = modelMapper.map(userRequest, User.class);

        String userId = UlidCreator.getUlid().toString();
        user.setUserId(userId);

        List<Shard> replicas = router.getReplicas(userId);
        log.info("Creating user {} → replicas {}", userId,
                replicas.stream().map(Shard::getShardId).toList());

        ShardOperationService.WriteResult result =
                shardOp.writeToReplicasWithQuorum(replicas, user);

        if (!result.isFullyReplicated()) {
            log.warn("User {} only replicated to {}/{} shards. Failed: {}",
                    userId, result.succeeded(), result.attempted(), result.failedShardIds());
        }

        return modelMapper.map(user, UserResponse.class);
    }


    // ═══════════════════════════════════════════════
    // READ — cache → primary → replicas → all shards
    // ═══════════════════════════════════════════════
    public UserResponse getUserById(String userId) {
        // 1. Try cache
        UserResponse cached = cacheService.get("user", userId, UserResponse.class);
        if (cached != null) {
            return cached;
        }

        // 2. Cache miss — try replicas in order (primary first)
        List<Shard> replicas = router.getReplicas(userId);
        for (Shard shard : replicas) {
            UserResponse response = readFromShardSafe(shard, userId);
            if (response != null) {
                cacheService.put("user", userId, response);
                return response;
            }
        }

        // 3. Fallback: try other shards not in the replica set
        Set<String> replicaIds = new HashSet<>();
        replicas.forEach(s -> replicaIds.add(s.getShardId()));

        for (Shard shard : router.getAllShards()) {
            if (replicaIds.contains(shard.getShardId())) continue;
            UserResponse response = readFromShardSafe(shard, userId);
            if (response != null) {
                log.warn("User {} found on non-replica shard {} — caching anyway",
                        userId, shard.getShardId());
                cacheService.put("user", userId, response);
                return response;
            }
        }

        // 4. Not found anywhere — don't cache negative results
        return null;
    }

    /**
     * Read from one shard, catching ANY exception.
     * Returns null on error — caller falls through to next replica.
     */
    private UserResponse readFromShardSafe(Shard shard, String userId) {
        ShardContext.setShard(shard.getShardId());
        try {
            Optional<User> userOptional = userRepository.findById(userId);
            return userOptional
                    .map(u -> modelMapper.map(u, UserResponse.class))
                    .orElse(null);
        } catch (Exception e) {
            log.warn("Read from shard {} failed for user {}: {}",
                    shard.getShardId(), userId, e.getMessage());
            return null;
        } finally {
            ShardContext.clear();
        }
    }

    // ═══════════════════════════════════════════════
    // DELETE — from all replicas
    // ═══════════════════════════════════════════════
    public boolean deleteUserById(String userId) {
        List<Shard> replicas = router.getReplicas(userId);

        // Check existence (safe version)
        boolean foundAnywhere = false;
        for (Shard shard : replicas) {
            if (existsInShardSafe(shard, userId)) {
                foundAnywhere = true;
                break;
            }
        }

        if (!foundAnywhere) {
            Set<String> replicaIds = new HashSet<>();
            replicas.forEach(s -> replicaIds.add(s.getShardId()));

            for (Shard shard : router.getAllShards()) {
                if (replicaIds.contains(shard.getShardId())) continue;
                if (existsInShardSafe(shard, userId)) {
                    foundAnywhere = true;
                    break;
                }
            }
        }

        if (!foundAnywhere) {
            return false;
        }

        // Delete from all replicas (already failure-tolerant inside shardOp)
        ShardOperationService.WriteResult result =
                shardOp.deleteFromReplicas(replicas, userId);

        log.info("Deleted user {} → {}/{} replicas (failed: {})",
                userId, result.succeeded(), result.attempted(), result.failedShardIds());

        if (result.succeeded() > 0) {
            cacheService.evict("user", userId);
        }
        return result.succeeded() > 0;
    }

    /**
     * Check existence safely — catch any exception and treat as "not found".
     */
    private boolean existsInShardSafe(Shard shard, String userId) {
        ShardContext.setShard(shard.getShardId());
        try {
            return userRepository.existsById(userId);
        } catch (Exception e) {
            log.warn("existsById on shard {} failed for user {}: {}",
                    shard.getShardId(), userId, e.getMessage());
            return false;
        } finally {
            ShardContext.clear();
        }
    }
}