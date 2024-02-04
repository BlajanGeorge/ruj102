package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String limiterKey = KeyHelper.getKey(String.format("limiter:%d:%s:%d", windowSizeMS, name, maxHits));
        try (Jedis jedis = jedisPool.getResource()) {
            try (Transaction transaction = jedis.multi()) {
                long now = System.currentTimeMillis();
                transaction.zadd(limiterKey, now, String.format("%d-%f", now, Math.random()));
                transaction.zremrangeByScore(limiterKey, 0, now - windowSizeMS);
                Response<Long> requests = transaction.zcard(limiterKey);
                transaction.exec();
                if (requests.get() > maxHits) throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
