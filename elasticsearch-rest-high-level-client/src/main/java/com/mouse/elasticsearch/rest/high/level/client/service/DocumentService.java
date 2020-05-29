package com.mouse.elasticsearch.rest.high.level.client.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RethrottleRequest;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/14
 * @description
 */
@Component
public class DocumentService {

    @Resource
    private RestHighLevelClient client;

    public IndexResponse index(IndexRequest indexRequest) throws IOException {
        return client.index(indexRequest, RequestOptions.DEFAULT);
    }

    public GetResponse get(GetRequest getRequest) throws IOException {
        return client.get(getRequest, RequestOptions.DEFAULT);
    }

    public boolean exists(GetRequest getRequest) throws IOException {
        //由于exists()仅返回true或false，因此建议关闭提取功能_source和所有存储的字段，这样请求的内容会稍微减轻一些
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    public DeleteResponse delete(DeleteRequest deleteRequest) throws IOException {
        return client.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    //todo 脚本 upsert
    public UpdateResponse update(UpdateRequest updateRequest) throws IOException {
        return client.update(updateRequest, RequestOptions.DEFAULT);
    }

    public void termvectors(TermVectorsRequest termVectorsRequest) throws IOException {
        client.termvectors(termVectorsRequest, RequestOptions.DEFAULT);
    }

    public void bulk(BulkRequest bulkRequest) throws IOException {
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    public void mget(MultiGetRequest multiGetRequest) throws IOException {
        client.mget(multiGetRequest, RequestOptions.DEFAULT);
    }

    public void submitReindexTask(ReindexRequest reindexRequest) throws IOException {
        client.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
    }

    public void updateByQuery(UpdateByQueryRequest updateByQueryRequest) throws IOException {
        client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
    }

    public void deleteByQuery(DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }

    public void reindexRethrottle(RethrottleRequest rethrottleRequest) throws IOException {
        client.reindexRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
        client.updateByQueryRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
        client.deleteByQueryRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
    }

    public void mtermvectors(MultiTermVectorsRequest multiTermVectorsRequest) throws IOException {
        client.mtermvectors(multiTermVectorsRequest, RequestOptions.DEFAULT);
    }

    public void indexAsync(IndexRequest indexRequest) throws IOException {
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

}
