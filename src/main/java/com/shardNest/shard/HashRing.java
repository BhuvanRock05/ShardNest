package com.shardNest.shard;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class HashRing {

    private final SortedMap<Integer, Shard> ring = new ConcurrentSkipListMap<>();

    public void addShard(Shard shard, int vnodeCount) {
        for (int i = 0; i < vnodeCount; i++) {
            String vnodeKey = shard.getShardId() + "#" + i;
            int position = hash(vnodeKey);
            ring.put(position, shard);
        }
    }

    public void removeShard(Shard shard, int vnodeCount) {
        for (int i = 0; i < vnodeCount; i++) {
            String vnodeKey = shard.getShardId() + "#" + i;
            ring.remove(hash(vnodeKey));
        }
    }

    public void addShard(Shard shard) {
        addShard(shard, 1);
    }

    public void removeShard(Shard shard) {
        removeShard(shard, 1);
    }

    public Shard getShard(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Hash ring is empty");
        }

        int keyHash = hash(key);
        SortedMap<Integer, Shard> tail = ring.tailMap(keyHash);
        Integer shardPosition = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(shardPosition);
    }

    /**
     * Hash function using Murmur3 for excellent distribution.
     *
     * Why not String.hashCode()?
     *  - String.hashCode() is linear: changing "shard1#0" → "shard1#1"
     *    only changes the hash by +1.
     *  - Result: VNodes cluster at consecutive positions instead of
     *    spreading across the ring.
     *  - Murmur3 has the "avalanche effect": one bit change flips ~50%
     *    of output bits, spreading VNodes evenly.
     */
    private int hash(String key) {
        return Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt() & 0x7fffffff;
    }

    public SortedMap<Integer, Shard> getRing() {
        return ring;
    }

    public int size() {
        return ring.size();
    }

    public List<Shard> getDistinctShards() {
        return ring.values().stream()
                .distinct()
                .toList();
    }

    public List<Shard> getShards(String key, int count) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Hash ring is empty");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        int keyHash = hash(key);
        List<Shard> result = new ArrayList<>(count);
        Set<String> seenShardIds = new HashSet<>();

        // Combine tailMap + headMap for a wrapped iteration
        List<Map.Entry<Integer, Shard>> combined = new ArrayList<>();
        combined.addAll(ring.tailMap(keyHash).entrySet());
        combined.addAll(ring.headMap(keyHash).entrySet());

        for (Map.Entry<Integer, Shard> entry : combined) {
            Shard shard = entry.getValue();
            if (seenShardIds.add(shard.getShardId())) {
                result.add(shard);
                if (result.size() == count) {
                    break;
                }
            }
        }

        return result;
    }
}
