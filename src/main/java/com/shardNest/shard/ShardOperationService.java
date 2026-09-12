package com.shardNest.shard;

import com.shardNest.config.ShardContext;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Wraps every shard operation in its own transaction, opened AFTER
 * ShardContext is set. This is critical because:
 *
 *  1) AbstractRoutingDataSource picks the DataSource when a Connection
 *     is requested — i.e. when the transaction begins.
 *
 *  2) @Transactional opens the transaction when the method is ENTERED,
 *     before any line of code (including setShard) runs.
 *
 *  3) TransactionTemplate lets us setShard FIRST, then start the tx.
 */
@Service
public class ShardOperationService {

    private final UserRepository userRepository;
    private final TransactionTemplate writeTx;
    private final TransactionTemplate readTx;

    public ShardOperationService(UserRepository userRepository,
                                 PlatformTransactionManager txManager) {
        this.userRepository = userRepository;

        this.writeTx = new TransactionTemplate(txManager);
        this.writeTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        this.readTx = new TransactionTemplate(txManager);
        this.readTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.readTx.setReadOnly(true);
    }

    public void saveToShard(String shardId, User user) {
        ShardContext.setShard(shardId);
        try {
            writeTx.execute(status -> {
                userRepository.saveAndFlush(user);   // ⭐ forces immediate INSERT
                return null;
            });
        } finally {
            ShardContext.clear();
        }
    }

    public boolean existsInShard(String shardId, String userId) {
        ShardContext.setShard(shardId);
        try {
            Boolean result = readTx.execute(status -> userRepository.existsById(userId));
            return Boolean.TRUE.equals(result);
        } finally {
            ShardContext.clear();
        }
    }

    public void deleteFromShard(String shardId, String userId) {
        ShardContext.setShard(shardId);
        try {
            writeTx.execute(status -> {
                userRepository.deleteById(userId);
                return null;
            });
        } finally {
            ShardContext.clear();
        }
    }

    public List<User> readPageFromShard(String shardId, String lastId, int pageSize) {
        ShardContext.setShard(shardId);
        try {
            return readTx.execute(status -> {
                if (lastId == null) {
                    return userRepository.findAll(
                            PageRequest.of(0, pageSize, Sort.by("userId"))
                    ).getContent();
                }
                return userRepository.findNextPage(lastId, PageRequest.of(0, pageSize));
            });
        } finally {
            ShardContext.clear();
        }
    }
}