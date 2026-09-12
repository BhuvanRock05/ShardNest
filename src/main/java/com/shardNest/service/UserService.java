package com.shardNest.service;

import com.github.f4b6a3.ulid.UlidCreator;
import com.shardNest.config.ShardContext;
import com.shardNest.dto.UserRequest;
import com.shardNest.dto.UserResponse;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import com.shardNest.shard.Shard;
import com.shardNest.shard.ShardRouter;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ShardRouter router;

    public UserResponse addUser(UserRequest userRequest) {
        User user = modelMapper.map(userRequest, User.class);

        String userId = UlidCreator.getUlid().toString();
        user.setUserId(userId);

        Shard shard = router.getShard(userId);
        ShardContext.setShard(shard.getShardId());

        try {
            User savedUser = userRepository.saveAndFlush(user);
            return modelMapper.map(savedUser, UserResponse.class);
        } finally {
            ShardContext.clear();
        }
    }

    /**
     * Read with fallback — tries primary shard, then all other shards.
     * Safe during migration.
     */
    public UserResponse getUserById(String userId) {
        Shard primary = router.getShard(userId);

        // 1. Try primary shard
        UserResponse response = readFromShard(primary, userId);
        if (response != null) return response;

        // 2. Fallback: try all other shards (during migration, data may not have moved yet)
        for (Shard shard : router.getAllShards()) {
            if (shard.getShardId().equals(primary.getShardId())) continue;
            response = readFromShard(shard, userId);
            if (response != null) return response;
        }

        return null;
    }

    private UserResponse readFromShard(Shard shard, String userId) {
        ShardContext.setShard(shard.getShardId());
        try {
            Optional<User> userOptional = userRepository.findById(userId);
            return userOptional
                    .map(u -> modelMapper.map(u, UserResponse.class))
                    .orElse(null);
        } finally {
            ShardContext.clear();
        }
    }

    /**
     * Delete with fallback — find where the user actually lives, then delete.
     */
    public boolean deleteUserById(String userId) {
        Shard primary = router.getShard(userId);

        // 1. Try primary
        if (deleteFromShard(primary, userId)) return true;

        // 2. Fallback: try other shards
        for (Shard shard : router.getAllShards()) {
            if (shard.getShardId().equals(primary.getShardId())) continue;
            if (deleteFromShard(shard, userId)) return true;
        }

        return false;
    }

    private boolean deleteFromShard(Shard shard, String userId) {
        ShardContext.setShard(shard.getShardId());
        try {
            if (userRepository.existsById(userId)) {
                userRepository.deleteById(userId);
                return true;
            }
            return false;
        } finally {
            ShardContext.clear();
        }
    }
}