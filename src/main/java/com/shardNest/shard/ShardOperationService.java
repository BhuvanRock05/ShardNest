package com.shardNest.shard;

import com.shardNest.config.ShardContext;
import com.shardNest.model.Address;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ShardOperationService {

    private final UserRepository userRepository;

    private static final int PAGE_SIZE = 500;

    public ShardOperationService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    void moveUser(User user, Shard from, Shard to) {

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

    // ═══════════════════════════════════════════════
    // HELPERS
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
}
