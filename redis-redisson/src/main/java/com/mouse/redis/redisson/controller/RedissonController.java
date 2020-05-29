package com.mouse.redis.redisson.controller;

import org.redisson.api.*;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/18
 * @description
 */
@RestController
@RequestMapping("/redisson")
public class RedissonController {

    @Resource
    private RedissonClient redissonClient;

    /**
     * string
     * set操作是覆盖操作
     *
     * @param key
     * @param value
     * @return
     */
    @PostMapping("/string/set")
    public String setString(@RequestParam String key, @RequestParam String value) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        bucket.set(value);
        return "ok";
    }

    /**
     * list
     * 相当于Java的List 有序可重复
     *
     * @param key
     * @param value
     * @return 成功返回true
     */
    @PostMapping("/list/set")
    public boolean setList(@RequestParam String key, @RequestParam String value) {
        RList<String> list = redissonClient.getList(key);
        return list.add(value);
    }

    /**
     * set
     * 相当于Java的Set 无序不可重复
     *
     * @param key
     * @param value
     * @return 成功返回true 失败(重复)返回false
     */
    @PostMapping("/set/set")
    public boolean setSet(@RequestParam String key, @RequestParam String value) {
        RSet<Object> set = redissonClient.getSet(key);
        return set.add(value);
    }

    /**
     * zset
     * 相当于Java的SortedSet 排序不可重复
     *
     * @param key
     * @param value
     * @return 成功返回true 失败(重复)返回false
     */
    @PostMapping("/zset/set")
    public boolean setZSet(@RequestParam String key, @RequestParam String value) {
        RSortedSet<String> sortedSet = redissonClient.getSortedSet(key);
        return sortedSet.add(value);
    }

    /**
     * hash
     * 相当于Java的Map 相同的mapKey 会覆盖value
     *
     * @param key
     * @param mapKey
     * @param value
     * @return mapKey不存在时返回空 mapKey存在时返回旧的value
     */
    @PostMapping("/hash/set")
    public String setHash(@RequestParam String key, @RequestParam String mapKey, @RequestParam String value) {
        RMap<String, String> map = redissonClient.getMap(key);
        return map.put(mapKey, value);
    }

    @GetMapping("/string/get")
    public String getString(@RequestParam String key) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }

    public void set() {
        RLock lock = redissonClient.getLock("");
        lock.lock();
        RBucket<Object> bucket = redissonClient.getBucket("");
        RList<Object> list = redissonClient.getList("");
        list.add("");
        RSet<Object> set = redissonClient.getSet("");
        RMap<Object, Object> map = redissonClient.getMap("");
        RSortedSet<Object> sortedSet = redissonClient.getSortedSet("");
        bucket.set("");
    }

}
