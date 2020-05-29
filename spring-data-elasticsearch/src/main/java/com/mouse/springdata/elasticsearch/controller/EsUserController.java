package com.mouse.springdata.elasticsearch.controller;

import com.mouse.springdata.elasticsearch.dataobject.UserDO;
import com.mouse.springdata.elasticsearch.repository.EsUserRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author mouse
 * @version 1.0
 * @date 2020-01-19
 * @description
 */
@RestController
public class EsUserController {

    @Resource
    private EsUserRepository esUserRepository;

    @PostMapping("/es/user/save")
    public UserDO save(@RequestParam String name, @RequestParam Integer age) {
        UserDO userDO = new UserDO();
        userDO.setUsername(name);
        userDO.setAge(age);
        return esUserRepository.save(userDO);
    }

    @GetMapping("/es/user/list")
    public Iterable<UserDO> list() {
        return esUserRepository.findAll();

    }

}
