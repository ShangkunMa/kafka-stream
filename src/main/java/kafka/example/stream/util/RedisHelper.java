package kafka.example.stream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by MaShangkun on 18-9-10.
 */
public class RedisHelper {
    private static RedisHelper helper = null;
    private static Object locker = new Object();

    private JedisPool cachePool = null;

    private Logger logger = null;

    /**
     * Singleton
     */
    private RedisHelper() {
        logger = LoggerFactory.getLogger(this.getClass().getName());

        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);

        cachePool = new JedisPool(
                config,
                Configurations.getInstance().cacheHost,
                Configurations.getInstance().cachePort
        );
    }

    /**
     * Get Singleton Instance of RedisHelper Class
     *
     * @return RedisHelper
     */
    public static final RedisHelper getInstance() {
        if (null == helper) {
            synchronized (locker) {
                if (null == helper) {
                    helper = new RedisHelper();
                }
            }
        }

        return helper;
    }

    /**
     * Get Jedis instance from cachePool
     *
     * @return
     */
    public Jedis getJedis() {
        Jedis jedis = null;
        try {
            jedis = this.cachePool.getResource();
        } catch (Exception e) {
            logger.warn("cannot get cache connection. exception " + e.getMessage());
            e.printStackTrace();
        }

        return jedis;
    }

    /**
     * Return Jedis instance to its pool.
     *
     * @param jedis
     */
    public void returnJedis(Jedis jedis) {
        if (null == jedis) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            logger.warn("Exception occurs when close Jedis connection");
            e.printStackTrace();
        }
    }
}