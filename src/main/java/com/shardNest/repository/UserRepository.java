package com.shardNest.repository;

import com.shardNest.model.User;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserRepository extends JpaRepository<User, String> {

    /**
     * Keyset pagination: get the next page of users after `lastId`.
     * Uses the primary key index → O(log N + pageSize) instead of O(offset).
     */
    @Query("SELECT u FROM users_table u WHERE u.userId > :lastId ORDER BY u.userId ASC")
    List<User> findNextPage(@Param("lastId") String lastId, Pageable pageable);
}
