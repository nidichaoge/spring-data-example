package com.mouse.springdata.redis.controller;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/18
 * @description
 */
@RestController
@RequestMapping("/lettuce")
public class LettuceController {

    @Resource
    private RedisTemplate redisTemplate;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

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
        stringRedisTemplate.opsForValue().set(key, value);
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
    public String setList(@RequestParam String key, @RequestParam String value) {
        stringRedisTemplate.opsForList().set(key, 0, value);
        return "ok";
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
    public Long setSet(@RequestParam String key, @RequestParam String value) {
        return stringRedisTemplate.opsForSet().add(key, value);
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
        return stringRedisTemplate.opsForZSet().add(key, value, 1);
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
        stringRedisTemplate.opsForHash().put(key, mapKey, value);
        return "ok";
    }

    @GetMapping("/string/get")
    public Object getString(@RequestParam String key) {
        return redisTemplate.opsForValue().get(key);
    }

}
