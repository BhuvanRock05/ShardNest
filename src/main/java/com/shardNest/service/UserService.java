package com.shardNest.service;

import com.shardNest.dto.UserRequest;
import com.shardNest.dto.UserResponse;
import com.shardNest.model.User;
import com.shardNest.repository.UserRepository;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private UserRepository userRepository;

    public UserResponse addUser(UserRequest userRequest) {

        User user = modelMapper.map(userRequest, User.class);

        User savedUser = userRepository.save(user);

        return modelMapper.map(savedUser, UserResponse.class);
    }
}
