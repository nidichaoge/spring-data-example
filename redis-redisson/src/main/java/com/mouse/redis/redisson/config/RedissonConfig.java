package com.mouse.redis.redisson.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/18
 * @description
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.redis.host}")
    private String host;
    @Value("${spring.redis.password}")
    private String password;
    @Value("${spring.redis.port}")
    private int port;
    @Value("${spring.redis.database}")
    private int database;

    @Bean
    public RedissonClient redissonClient(){
        Config config = new Config();
        SingleServerConfig singleServerConfig = config.useSingleServer();

        singleServerConfig.setAddress("redis://"+host+":"+port);
//        singleServerConfig.setPassword(password);
        singleServerConfig.setDatabase(database);
//        singleServerConfig.setConnectionPoolSize(15);
//        singleServerConfig.setConnectionMinimumIdleSize(5);
//        singleServerConfig.setRetryAttempts(10);
//        singleServerConfig.setRetryInterval(10_000);
//        singleServerConfig.setPingTimeout(30_000);
//        singleServerConfig.setPingConnectionInterval(5000);
        return Redisson.create(config);
    }

}
