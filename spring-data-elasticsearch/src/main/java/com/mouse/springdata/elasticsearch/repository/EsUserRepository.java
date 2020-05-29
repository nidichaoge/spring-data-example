package com.mouse.springdata.elasticsearch.repository;

import com.mouse.springdata.elasticsearch.dataobject.UserDO;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * @author mouse
 * @version 1.0
 * @date 2020-01-19
 * @description
 */
public interface EsUserRepository extends ElasticsearchRepository<UserDO,Long> {
}
