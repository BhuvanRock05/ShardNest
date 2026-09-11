package com.shardNest.controller;


import com.shardNest.shard.ConsistentHashRouter;
import com.shardNest.shard.HashRing;
import com.shardNest.shard.Shard;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/debug")
public class ShardDebugController {

    @Autowired
    private ConsistentHashRouter consistentHashRouter;

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

}