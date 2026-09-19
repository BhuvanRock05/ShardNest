package com.shardNest.controller;

import com.shardNest.cache.CacheService;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/debug/cache")
public class CacheDebugController {

    private final CacheService cacheService;
    private final RedisTemplate<String, Object> redisTemplate;

    public CacheDebugController(CacheService cacheService,
                                RedisTemplate<String, Object> redisTemplate) {
        this.cacheService = cacheService;
        this.redisTemplate = redisTemplate;
    }

    // ═══════════════════════════════════════════════
    // Health
    // ═══════════════════════════════════════════════
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", cacheService.isEnabled());
        result.put("healthy", cacheService.isHealthy());
        result.put("keyPrefix", cacheService.getKeyPrefix());
        result.put("defaultTtlSeconds", cacheService.getDefaultTtlSeconds());
        return result;
    }

    // ═══════════════════════════════════════════════
    // Inspect a user's cache entry
    // ═══════════════════════════════════════════════
    @GetMapping("/user/{userId}")
    public Map<String, Object> getUser(@PathVariable String userId) {
        String key = cacheService.getKeyPrefix() + "user:" + userId;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);

        try {
            Object value = redisTemplate.opsForValue().get(key);
            result.put("exists", value != null);
            result.put("value", value);
            result.put("ttl", value != null ? redisTemplate.getExpire(key) : -2);
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }

        return result;
    }

    // ═══════════════════════════════════════════════
    // Manually evict
    // ═══════════════════════════════════════════════
    @DeleteMapping("/user/{userId}")
    public Map<String, Object> evictUser(@PathVariable String userId) {
        cacheService.evict("user", userId);
        return Map.of("evicted", true, "userId", userId);
    }
}