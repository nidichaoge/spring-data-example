package com.mouse.elasticsearch.rest.high.level.client.controller;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/18
 * @description
 */
@RestController
@RequestMapping("/mapping")
public class MappingController {

    @Resource
    private RestHighLevelClient client;

    public void create(){

    }
}
