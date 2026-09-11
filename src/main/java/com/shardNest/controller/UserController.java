package com.shardNest.controller;

import com.shardNest.dto.UserRequest;
import com.shardNest.dto.UserResponse;
import com.shardNest.service.UserService;
import com.shardNest.shard.Shard;
import com.shardNest.shard.ShardRouter;
import com.shardNest.shard.SimpleShardRouter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/user")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/create")
    public ResponseEntity<UserResponse> createUser(@RequestBody UserRequest userRequest) {
        UserResponse response = userService.addUser(userRequest);

        return ResponseEntity.ok(response);
    }

    @GetMapping("/{userId}")
    public ResponseEntity<UserResponse> getUserById(@PathVariable String userId) {
        UserResponse response = userService.getUserById(userId);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/delete/{userId}")
    public ResponseEntity<Void> deleteUserById(@PathVariable String userId) {
        boolean deleted = userService.deleteUserById(userId);
        if (!deleted) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/test")
    public void testShard() {
        List<Shard> shards = List.of(
                new Shard("shard-1", "shard_db_1"),
                new Shard("shard-2", "shard_db_2"),
                new Shard("shard-3", "shard_db_3")
        );

        ShardRouter router = new SimpleShardRouter(shards);

//        for (int i = 1; i <= 20; i++) {
//
//            String key = "user-" + i;
//
//            Shard shard = router.getShard(key);
//
//            System.out.println(
//                    key + " -> " + shard.getShardId()
//            );
//        }
    }

}
