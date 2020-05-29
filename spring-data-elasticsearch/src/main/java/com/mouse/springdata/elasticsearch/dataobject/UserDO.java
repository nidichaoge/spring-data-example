package com.mouse.springdata.elasticsearch.dataobject;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * @author mouse
 * @version 1.0
 * @date 2020-01-19
 * @description
 */
@Data
@Document(indexName = "user",type = "docs",shards = 1,replicas = 0)
public class UserDO {

    @Id
    private Long id;

    private String username;

    private Integer age;

}
