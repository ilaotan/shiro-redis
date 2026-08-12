package org.crazycake.shiro;

import org.crazycake.shiro.common.WorkAloneRedisManager;

import com.alibaba.nacos.api.config.annotation.NacosValue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

public class RedisManagerDynamic extends WorkAloneRedisManager implements IRedisManager {

	private static final String DEFAULT_HOST = "127.0.0.1:6379";

	/** A 数据源地址 */
	private String host = DEFAULT_HOST;

	/** B 数据源地址，第二套数据源 */
	private String hostB = "";

	// timeout for jedis try to connect to redis server, not expire time! In milliseconds
	private int timeout = Protocol.DEFAULT_TIMEOUT;

	private String password;

	private int database = Protocol.DEFAULT_DATABASE;

	/** A 数据源连接池 */
	private volatile JedisPool jedisPool;

	/** B 数据源连接池 */
	private volatile JedisPool jedisPoolB;

	/** 第二套数据源是否已配置，hostB 注入时计算一次，避免每次切换判断 */
	private boolean secondDataSourceConfigured;

	/**
	 * true时使用B数据源，默认false使用A数据源，由Nacos动态刷新
	 */
	@NacosValue(value = "${spring.redis.useNewClient:false}", autoRefreshed = true)
	private volatile boolean useNewClient;

	private void init() {
		// 初始化 A 数据源
		if (jedisPool == null) {
			synchronized (RedisManagerDynamic.class) {
				if (jedisPool == null) {
					jedisPool = createPool(host);
				}
			}
		}
		// 第二套数据源存在时才初始化 B 数据源
		if (secondDataSourceConfigured && jedisPoolB == null) {
			synchronized (RedisManagerDynamic.class) {
				if (jedisPoolB == null) {
					jedisPoolB = createPool(hostB);
				}
			}
		}
	}

	private JedisPool createPool(String hostAndPort) {
		String[] hostAndPortArr = hostAndPort.split(":");
		return new JedisPool(getJedisPoolConfig(), hostAndPortArr[0], Integer.parseInt(hostAndPortArr[1]), timeout, password, database);
	}

	@Override
	protected Jedis getJedis() {
		// 动态切换：useNewClient=true 且第二套数据源存在时使用 B 数据源，否则使用 A 数据源
		if (useNewClient && secondDataSourceConfigured) {
			init();
			return jedisPoolB.getResource();
		}
		init();
		return jedisPool.getResource();
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getHostB() {
		return hostB;
	}

	public void setHostB(String hostB) {
		this.hostB = hostB;
		// 注入时一次性判断第二套数据源是否已配置，避免每次切换重复判断
		this.secondDataSourceConfigured = hostB != null && !hostB.trim().isEmpty();
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	public JedisPool getJedisPoolB() {
		return jedisPoolB;
	}

	public void setJedisPoolB(JedisPool jedisPoolB) {
		this.jedisPoolB = jedisPoolB;
	}
}
