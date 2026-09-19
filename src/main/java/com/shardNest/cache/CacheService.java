package com.shardNest.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Simple fail-open cache wrapper for Redis.
 *
 * - Every operation is wrapped in try/catch.
 * - Any Redis failure → logged, returns null (for GET) or does nothing (for PUT/EVICT).
 * - The application never fails because of cache issues.
 *
 * No circuit breaker. If Redis is down, every call pays the
 * Lettuce timeout (~2s) before falling back.
 */
@Service
public class CacheService {

    private static final Logger log = LoggerFactory.getLogger(CacheService.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final String keyPrefix;
    private final Duration defaultTtl;
    private final boolean enabled;

    public CacheService(
            RedisTemplate<String, Object> redisTemplate,
            @Value("${shard.cache.enabled:true}") boolean enabled,
            @Value("${shard.cache.key-prefix:shardnest:}") String keyPrefix,
            @Value("${shard.cache.ttl-seconds:600}") long ttlSeconds) {
        this.redisTemplate = redisTemplate;
        this.enabled = enabled;
        this.keyPrefix = keyPrefix;
        this.defaultTtl = Duration.ofSeconds(ttlSeconds);
    }

    // ═══════════════════════════════════════════════
    // GET
    // ═══════════════════════════════════════════════
    @SuppressWarnings("unchecked")
    public <T> T get(String namespace, String key, Class<T> type) {
        if (!enabled) return null;

        String fullKey = buildKey(namespace, key);
        long start = System.nanoTime();

        try {
            Object value = redisTemplate.opsForValue().get(fullKey);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            if (value == null) {
                log.info("CACHE MISS: {} ({}ms)", fullKey, elapsedMs);
                return null;
            }
            log.info("CACHE HIT: {} ({}ms)", fullKey, elapsedMs);
            return (T) value;

        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.warn("Cache GET failed for {} ({}ms): {}", fullKey, elapsedMs, e.getMessage());
            return null;
        }
    }

    // ═══════════════════════════════════════════════
    // PUT
    // ═══════════════════════════════════════════════
    public void put(String namespace, String key, Object value) {
        put(namespace, key, value, defaultTtl);
    }

    public void put(String namespace, String key, Object value, Duration ttl) {
        if (!enabled) return;

        String fullKey = buildKey(namespace, key);
        long start = System.nanoTime();

        try {
            redisTemplate.opsForValue().set(fullKey, value, ttl);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.info("CACHE PUT: {} ({}ms, ttl={}s)", fullKey, elapsedMs, ttl.getSeconds());

        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.warn("Cache PUT failed for {} ({}ms): {}", fullKey, elapsedMs, e.getMessage());
        }
    }

    // ═══════════════════════════════════════════════
    // EVICT
    // ═══════════════════════════════════════════════
    public void evict(String namespace, String key) {
        if (!enabled) return;

        String fullKey = buildKey(namespace, key);
        long start = System.nanoTime();

        try {
            Boolean deleted = redisTemplate.delete(fullKey);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.info("CACHE EVICT: {} ({}ms, deleted={})", fullKey, elapsedMs, deleted);

        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.warn("Cache EVICT failed for {} ({}ms): {}", fullKey, elapsedMs, e.getMessage());
        }
    }

    // ═══════════════════════════════════════════════
    // HEALTH
    // ═══════════════════════════════════════════════
    public boolean isHealthy() {
        if (!enabled) return false;

        try {
            redisTemplate.getConnectionFactory().getConnection().ping();
            return true;
        } catch (Exception e) {
            log.warn("Redis health check failed: {}", e.getMessage());
            return false;
        }
    }

    // ═══════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════
    private String buildKey(String namespace, String key) {
        return keyPrefix + namespace + ":" + key;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public long getDefaultTtlSeconds() {
        return defaultTtl.getSeconds();
    }

    public boolean isEnabled() {
        return enabled;
    }
}