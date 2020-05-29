package com.mouse.elasticsearch.rest.high.level.client.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/14
 * @description
 */
@Data
public class DocumentDTO implements Serializable {

    private static final long serialVersionUID = 5069093966615109674L;

    private String id;

    private String string;

    private Integer integer;

    private List<String> stringList;

    private Boolean exist;

}
