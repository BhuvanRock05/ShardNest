package com.shardNest.controller;

import com.shardNest.dto.UserRequest;
import com.shardNest.dto.UserResponse;
import com.shardNest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<UserResponse> getUserById(@PathVariable Long userId) {
        UserResponse response = userService.getUserById(userId);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

}
