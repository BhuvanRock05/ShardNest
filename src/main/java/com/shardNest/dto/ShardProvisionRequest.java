package com.shardNest.dto;

import lombok.Data;

@Data
public class ShardProvisionRequest {

    private String shardId;                 // "shard5"
    private String jdbcUrl;                 // "jdbc:mysql://localhost:3306/userdb5"
    private String username;
    private String password;
    private String driverClassName;         // "com.mysql.cj.jdbc.Driver"

    private boolean createDatabase = true;  // CREATE DATABASE IF NOT EXISTS
    private String databaseName;            // "userdb5" — required if createDatabase=true

    private boolean createTables = true;    // CREATE TABLE IF NOT EXISTS
}