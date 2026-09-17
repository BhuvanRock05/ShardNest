package com.shardNest.controller;


import com.shardNest.config.ShardContext;
import com.shardNest.model.Address;
import com.shardNest.model.User;
import com.shardNest.shard.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/debug")
public class ShardDebugController {

    @Autowired
    private ConsistentHashRouter consistentHashRouter;

    @Autowired
    private ShardOperationService shardOp;

    @Autowired
    private ShardRouter shardRouter;   // ← Add field

    /**
     * Show the FULL ring (all VNodes) — will be huge with 300 positions!
     * Use /ring/sample instead for a summary view.
     */
    @GetMapping("/ring")
    public Map<Integer, String> showRing() {
        HashRing ring = consistentHashRouter.getRing();
        Map<Integer, String> view = new TreeMap<>();
        ring.getRing().forEach((pos, shard) -> view.put(pos, shard.getShardId()));
        return view;
    }

    /**
     * Summary: how many VNodes does each shard have?
     */
    @GetMapping("/ring/summary")
    public Map<String, Object> ringSummary() {
        HashRing ring = consistentHashRouter.getRing();
        Map<String, Integer> counts = new TreeMap<>();
        ring.getRing().values().forEach(shard ->
                counts.merge(shard.getShardId(), 1, Integer::sum)
        );

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalVNodes", ring.size());
        summary.put("vnodeCountPerShard", consistentHashRouter.getVnodeCount());
        summary.put("vnodeDistribution", counts);
        return summary;
    }

    /**
     * Show first N VNodes (for quick inspection).
     */
    @GetMapping("/ring/sample")
    public Map<Integer, String> sampleRing(@RequestParam(defaultValue = "20") int limit) {
        HashRing ring = consistentHashRouter.getRing();
        Map<Integer, String> view = new TreeMap<>();
        ring.getRing().entrySet().stream()
                .limit(limit)
                .forEach(e -> view.put(e.getKey(), e.getValue().getShardId()));
        return view;
    }

    /**
     * Route a single key.
     */
    @GetMapping("/route/{key}")
    public String route(@PathVariable String key) {
        Shard shard = consistentHashRouter.getShard(key);
        return key + " → " + shard.getShardId() + " (" + shard.getDatabaseName() + ")";
    }

    /**
     * Distribution test — the BIG MOMENT!
     * Compare this with Phase 4's output.
     */
    @GetMapping("/distribution/{count}")
    public Map<String, Object> distribution(@PathVariable int count) {
        Map<String, Integer> dist = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            String key = "user-" + i;
            Shard shard = consistentHashRouter.getShard(key);
            dist.merge(shard.getShardId(), 1, Integer::sum);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalKeys", count);
        result.put("vnodeCountPerShard", consistentHashRouter.getVnodeCount());
        result.put("distribution", dist);
        result.put("percentages", computePercentages(dist, count));
        return result;
    }

    private Map<String, String> computePercentages(Map<String, Integer> dist, int total) {
        Map<String, String> pct = new TreeMap<>();
        dist.forEach((shard, count) ->
                pct.put(shard, String.format("%.2f%%", 100.0 * count / total))
        );
        return pct;
    }

    @GetMapping("/test/migration")
    public void testMigrationWithVNodes() {
        List<Shard> shards3 = List.of(
                new Shard("shard1", "userdb1"),
                new Shard("shard2", "userdb2"),
                new Shard("shard3", "userdb3")
        );
        ConsistentHashRouter router = new ConsistentHashRouter(shards3, 100);

        // Record routing for 10,000 keys
        Map<String, String> before = new HashMap<>();
        int N = 10_000;
        for (int i = 0; i < N; i++) {
            String key = "user-" + i;
            before.put(key, router.getShard(key).getShardId());
        }

        // Add a 4th shard with VNodes
        router.addShard(new Shard("shard4", "userdb4"));

        // Count moved keys
        int moved = 0;
        for (int i = 0; i < N; i++) {
            String key = "user-" + i;
            String after = router.getShard(key).getShardId();
            if (!after.equals(before.get(key))) moved++;
        }

        System.out.printf("After adding shard4: %d/%d moved (%.2f%%)%n",
                moved, N, 100.0 * moved / N);
        // Expected: ~25% (1/4 of keys move to new shard)
    }

    @GetMapping("/replicas/{key}")
    public Map<String, Object> showReplicas(@PathVariable String key,
                                            @RequestParam(required = false) Integer rf) {
        List<Shard> shards = (rf != null)
                ? consistentHashRouter.getReplicas(key, rf)
                : consistentHashRouter.getReplicas(key);

        List<String> shardIds = shards.stream()
                .map(Shard::getShardId)
                .toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("rf", shards.size());
        result.put("primary", shardIds.isEmpty() ? null : shardIds.get(0));
        result.put("replicas", shardIds.size() > 1
                ? shardIds.subList(1, shardIds.size())
                : List.of());
        result.put("all", shardIds);
        return result;
    }

    @GetMapping("/consistency-check/{key}")
    public Map<String, Object> consistencyCheck(@PathVariable String key) {
        HashRing ring = consistentHashRouter.getRing();

        // Get the ring's raw state
        Map<Integer, String> rawRing = new TreeMap<>();
        ring.getRing().forEach((pos, shard) -> rawRing.put(pos, shard.getShardId()));

        // Path A
        Shard viaGetShard = ring.getShard(key);

        // Path B
        List<Shard> viaGetShards = ring.getShards(key, 3);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("ringSize", ring.size());
        result.put("getShard", viaGetShard.getShardId());
        result.put("getShards_all", viaGetShards.stream().map(Shard::getShardId).toList());
        result.put("getShards_primary", viaGetShards.get(0).getShardId());
        result.put("match", viaGetShard.getShardId().equals(viaGetShards.get(0).getShardId()));
        result.put("rawRingSize", rawRing.size());
        return result;
    }


    @PostMapping("/test-write-replicas/{key}")
    public Map<String, Object> testWriteReplicas(@PathVariable String key) {
        // Build a fake user
        User u = new User();
        u.setUserId(key);
        u.setUserName("test-user");
        u.setPassword("test");
        u.setFirstName("Test");
        u.setLastName("User");
        u.setEmail(key + "@test.com");
        u.setPhone("0000000000");

        Address addr = new Address();
        addr.setStreet("123 Test St");
        addr.setCity("Testville");
        addr.setState("TS");
        addr.setCountry("Testland");
        addr.setZipCode("00000");
        u.setAddress(addr);

        // Get replicas from router
        List<Shard> replicas = consistentHashRouter.getReplicas(key);

        // Write to all
        ShardOperationService.WriteResult result = shardOp.writeToReplicas(replicas, u);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("key", key);
        response.put("replicas", replicas.stream().map(Shard::getShardId).toList());
        response.put("attempted", result.attempted());
        response.put("succeeded", result.succeeded());
        response.put("failed", result.failedShardIds());
        response.put("fullyReplicated", result.isFullyReplicated());
        return response;
    }


    @GetMapping("/user-locations/{userId}")
    public Map<String, Object> userLocations(@PathVariable String userId) {

        // Expected: where the ring says the user should be
        List<String> expected = consistentHashRouter.getReplicas(userId).stream()
                .map(Shard::getShardId)
                .toList();

        // Actual: where the user really exists
        List<String> actual = new ArrayList<>();
        for (Shard shard : consistentHashRouter.getAllShards()) {
            if (shardOp.existsOnShard(userId, shard.getShardId())) {
                actual.add(shard.getShardId());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("expectedReplicas", expected);
        result.put("actualLocations", actual);
        result.put("inSync", new HashSet<>(expected).equals(new HashSet<>(actual)));
        return result;
    }

}