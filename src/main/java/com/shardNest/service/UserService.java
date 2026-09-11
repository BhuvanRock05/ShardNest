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
        System.out.println("Selected - " + shard);

        try {
            // Save into selected shard
            User savedUser = userRepository.save(user);

            return modelMapper.map(savedUser, UserResponse.class);

        } finally {

            // Very important with ThreadLocal
            ShardContext.clear();
        }
    }

    public UserResponse getUserById(String userId) {
        Shard shard = router.getShard(userId);
        ShardContext.setShard(shard.getShardId());
        System.out.println("Selected - " + shard);

        try {
            Optional<User> userOptional = userRepository.findById(userId);
            return modelMapper.map(userOptional, UserResponse.class);
        } finally {
            ShardContext.clear();  // ← CRITICAL
        }

    }

    public boolean deleteUserById(String userId) {
        Shard shard = router.getShard(userId);
        ShardContext.setShard(shard.getShardId());
        System.out.println("Selected - " + shard);
        try {
            if (userRepository.existsById(userId)) {
                userRepository.deleteById(userId);
                return true;
            }
            return false;
        } finally {
            ShardContext.clear();  // ← CRITICAL
        }
    }
}
