package com.mouse.elasticsearch.rest.high.level.client.controller;

import com.google.gson.Gson;
import com.mouse.elasticsearch.rest.high.level.client.constants.EsConstants;
import com.mouse.elasticsearch.rest.high.level.client.dto.DocumentDTO;
import com.mouse.elasticsearch.rest.high.level.client.service.DocumentService;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RethrottleRequest;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author mouse
 * @version 1.0
 * @date 2020/4/13
 * @description document api
 * index函数 insert操作, 如果id重复则全量更新
 * update函数 update操作, 默认是与旧文档合并, 如果需要全量更新 使用index函数; 默认更新不存在的文档会抛异常 可通过upsert()函数指定更新不存在的文档就写入
 * delete函数
 * bulk函数
 * <p>
 * get函数
 * mget函数
 * exists函数
 */
@RestController
@RequestMapping("/document")
public class DocumentController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentController.class);

    @Resource
    private RestHighLevelClient client;

    @Resource
    private DocumentService documentService;

    private static final Gson gson = new Gson();

    /**
     * 支持 String Map XContentBuilder Object 4种
     * 支持同步和异步调用
     * <p>
     * 文档写入时, mapping不存在会自动创建
     * <p>
     * 默认情况下每次调用都会生成新文档, id由es自动生成
     * <p>
     * 每次对相同id的操作会使得version+1, 可用来做全量更新
     * 推荐: 必须设置id 方便根据id查询 此时的策略是存在就更新 根据id更新其他所有字段
     *
     * @param documentDTO
     * @return
     * @throws IOException
     */
    @PostMapping("/index")
    public String index(@RequestBody DocumentDTO documentDTO) throws IOException {
        String body = gson.toJson(documentDTO);
        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME);
        indexRequest.id(documentDTO.getId());
        indexRequest.source(body, XContentType.JSON);
//        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        RestStatus status = indexResponse.status();
        LOGGER.info("indexResponse:{}, status:{}.", indexResponse, status);

        IndexRequest indexRequest1 = generateWithString();
        IndexRequest indexRequest2 = generateWithMap();
        IndexRequest indexRequest3 = generateWithXContentBuilder();
        IndexRequest indexRequest4 = generateWithObject();
//        IndexResponse indexResponse1 = client.index(indexRequest1, RequestOptions.DEFAULT);
//        IndexResponse indexResponse2 = client.index(indexRequest2, RequestOptions.DEFAULT);
//        IndexResponse indexResponse3 = client.index(indexRequest3, RequestOptions.DEFAULT);
//        IndexResponse indexResponse4 = client.index(indexRequest4, RequestOptions.DEFAULT);

        return "ok";
    }

    private IndexRequest generateWithString() {
        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME);
        indexRequest.id("1");
        String jsonString = "{" +
                "\"user\":\"mouse\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        indexRequest.source(jsonString, XContentType.JSON);
        return indexRequest;
    }

    private IndexRequest generateWithMap() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "mouse");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME)
                .id("1").source(jsonMap);
        return indexRequest;
    }

    private IndexRequest generateWithXContentBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "mouse");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME)
                .id("1").source(builder);
        return indexRequest;
    }

    private IndexRequest generateWithObject() {
        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME)
                .id("1")
                .source("user", "mouse",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
        return indexRequest;
    }

    //-------------------------------------------------------------------------

    /**
     * 根据 id 查找
     *
     * @param documentId
     * @return
     * @throws IOException
     */
    @GetMapping("/get")
    public DocumentDTO get(@RequestParam String documentId) throws IOException {
        GetRequest getRequest = new GetRequest(EsConstants.INDEX_NAME, documentId);
//        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        LOGGER.info("getResponse:{}.", getResponse);
        if (getResponse.isExists()) {
            String sourceAsString = getResponse.getSourceAsString();
            return gson.fromJson(sourceAsString, DocumentDTO.class);
        }
        return null;
    }

    //-------------------------------------------------------------------------

    /**
     * 判断是否存在
     *
     * @param documentId
     * @return
     * @throws IOException
     */
    @GetMapping("/exists")
    public Boolean exists(@RequestParam String documentId) throws IOException {
        GetRequest getRequest = new GetRequest(EsConstants.INDEX_NAME, documentId);
        //由于exists()仅返回true或false，因此建议您关闭提取功能_source和所有存储的字段，这样请求的内容会稍微减轻一些
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    //-------------------------------------------------------------------------

    /**
     * 删除成功 status:OK
     * 删除不存在的数据 status:NOT_FOUND
     *
     * @param documentId
     * @return
     * @throws IOException
     */
    @DeleteMapping("/delete")
    public String delete(@RequestParam String documentId) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(EsConstants.INDEX_NAME, documentId);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        RestStatus status = deleteResponse.status();
        LOGGER.info("deleteResponse:{}, status:{}.", deleteResponse, status);
        return "ok";
    }

    //-------------------------------------------------------------------------

    /**
     * TODO 允许使用脚本来更新文档
     * 如何全量更新? 使用index函数使得对相同id的操作是全量更新
     * 更新成功 status:OK 只有文档有变更 version才会增加
     * <p>
     * 默认情况下 更新不存在的文档会抛异常 ElasticsearchStatusException. 可以通过upsert()方法使得更新不存在的文档就创建 此时status:CREATED
     * 没有值的字段不会更新为null, 即依然保留原文档的值
     *
     * @param documentDTO 当对部分文档使用更新时，该部分文档将与现有文档合并。
     * @return
     * @throws IOException
     */
    @PutMapping("/update")
    public String update(@RequestBody DocumentDTO documentDTO) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(EsConstants.INDEX_NAME, documentDTO.getId());
        String body = gson.toJson(documentDTO);
        updateRequest.doc(body, XContentType.JSON);
//        updateRequest.upsert(body,XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        RestStatus status = updateResponse.status();
        LOGGER.info("updateResponse:{}, status:{}.", updateResponse, status);
        return "ok";
    }

    //-------------------------------------------------------------------------

    /**
     * TODO 什么作用
     *
     * @param documentId
     * @return
     * @throws IOException
     */
    @GetMapping("/termvectors")
    public String termvectors(@RequestParam String documentId) throws IOException {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(EsConstants.INDEX_NAME, documentId);
        termVectorsRequest.setFields("string");
        TermVectorsResponse termVectorsResponse = client.termvectors(termVectorsRequest, RequestOptions.DEFAULT);
        List<TermVectorsResponse.TermVector> termVectorsList = termVectorsResponse.getTermVectorsList();
        LOGGER.info("termVectorsResponse:{}, termVectorsList:{}.", gson.toJson(termVectorsResponse), gson.toJson(termVectorsList));
        for (TermVectorsResponse.TermVector tv : termVectorsList) {
            String fieldname = tv.getFieldName(); // The name of the current field
            int docCount = tv.getFieldStatistics().getDocCount(); // Fields statistics for the current field - document count
            long sumTotalTermFreq = tv.getFieldStatistics().getSumTotalTermFreq(); // Fields statistics for the current field - sum of total term frequencies
            long sumDocFreq = tv.getFieldStatistics().getSumDocFreq(); // Fields statistics for the current field - sum of document frequencies
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms = tv.getTerms(); // Terms for the current field
                for (TermVectorsResponse.TermVector.Term term : terms) {
                    String termStr = term.getTerm(); // The name of the term
                    int termFreq = term.getTermFreq(); // Term frequency of the term
                    int docFreq = term.getDocFreq(); // Document frequency of the term
                    long totalTermFreq = term.getTotalTermFreq();// Total term frequency of the term
                    float score = term.getScore(); // Score of the term
                    if (term.getTokens() != null) {
                        List<TermVectorsResponse.TermVector.Token> tokens = term.getTokens(); // Tokens of the term
                        for (TermVectorsResponse.TermVector.Token token : tokens) {
                            int position = token.getPosition(); // Position of the token
                            int startOffset = token.getStartOffset(); // Start offset of the token
                            int endOffset = token.getEndOffset(); // End offset of the token
                            String payload = token.getPayload(); // Payload of the token
                        }
                    }
                }
            }
        }
        return "ok";
    }

    //-------------------------------------------------------------------------

    /**
     * 一个 BulkRequest 至少与需要添加一个操作, 支持index update delete
     * The Bulk API supports only documents encoded in JSON or SMILE
     * 一次请求可以携带多少个操作? 官方建议是批处理1000至5000个文档，总有效负载在5MB至15MB之间
     *
     * @param documentDTOList
     * @return
     * @throws IOException
     */
    @PostMapping("/bulk")
    public String bulk(@RequestBody List<DocumentDTO> documentDTOList) throws IOException {
        DocumentDTO index = documentDTOList.get(0);
        DocumentDTO update = documentDTOList.get(1);
        DocumentDTO delete = documentDTOList.get(2);

        IndexRequest indexRequest = new IndexRequest(EsConstants.INDEX_NAME);
        indexRequest.id(index.getId());
        indexRequest.source(gson.toJson(index), XContentType.JSON);

        UpdateRequest updateRequest = new UpdateRequest(EsConstants.INDEX_NAME, update.getId());
        updateRequest.doc(gson.toJson(update), XContentType.JSON);

        DeleteRequest deleteRequest = new DeleteRequest(EsConstants.INDEX_NAME, delete.getId());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequest);
        bulkRequest.add(updateRequest);
        bulkRequest.add(deleteRequest);

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        boolean hasFailures = bulkResponse.hasFailures();
        LOGGER.info("bulkResponse:{}, hasFailures:{}.", gson.toJson(bulkResponse), hasFailures);
        return "ok";
    }

    //-------------------------------------------------------------------------

    /**
     * 一次请求可以携带多少个操作? 官方建议是批处理1000至5000个文档，总有效负载在5MB至15MB之间
     *
     * @param documentIds
     * @return
     * @throws IOException
     */
    @GetMapping("/mget")
    public List<DocumentDTO> mget(String documentIds) throws IOException {
        String[] split = documentIds.split(",");
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        Arrays.stream(split).forEach(documentId -> multiGetRequest.add(new MultiGetRequest.Item(EsConstants.INDEX_NAME, documentId)));
        MultiGetResponse multiGetResponse = client.mget(multiGetRequest, RequestOptions.DEFAULT);
        LOGGER.info("multiGetResponse:{}.", gson.toJson(multiGetResponse));

        List<DocumentDTO> result = new ArrayList<>();
        MultiGetItemResponse[] multiGetItemResponses = multiGetResponse.getResponses();
        for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
            if (!multiGetItemResponse.isFailed()) {
                GetResponse response = multiGetItemResponse.getResponse();
                String sourceAsString = response.getSourceAsString();
                if (StringUtils.hasLength(sourceAsString)) {
                    DocumentDTO documentDTO = gson.fromJson(sourceAsString, DocumentDTO.class);
                    result.add(documentDTO);
                }
            }
        }

        return result;
    }

    //-------------------------------------------------------------------------

    /**
     * 从一个或多个索引文件复制到目标索引
     * <p>
     * bulkByScrollResponse 不能直接使用Gson 否则会抛如下异常
     * java.lang.IllegalArgumentException: Infinity is not a valid double value as per JSON specification. To override this behavior, use GsonBuilder.serializeSpecialFloatingPointValues() method.
     *
     * @param sourceIndex
     * @param targetIndex
     * @return
     * @throws IOException
     */
    @PostMapping("/reindex")
    public String reindex(@RequestParam String sourceIndex, @RequestParam String targetIndex) throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(targetIndex);
        BulkByScrollResponse bulkByScrollResponse = client.reindex(reindexRequest, RequestOptions.DEFAULT);
        LOGGER.info("bulkByScrollResponse:{}.", bulkByScrollResponse);
        return "ok";
    }

    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------

    public void updateByQuery() throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        LOGGER.info("bulkByScrollResponse:{}.", bulkByScrollResponse);
    }

    public void deleteByQuery() throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        LOGGER.info("bulkByScrollResponse:{}.", bulkByScrollResponse);
    }

    public String rethrottle() throws IOException {
        RethrottleRequest rethrottleRequest = new RethrottleRequest(TaskId.EMPTY_TASK_ID);

        ListTasksResponse reindexRethrottle = client.reindexRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
        ListTasksResponse updateByQueryRethrottle = client.updateByQueryRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
        ListTasksResponse deleteByQueryRethrottle = client.deleteByQueryRethrottle(rethrottleRequest, RequestOptions.DEFAULT);
        LOGGER.info("reindexRethrottle:{}.", reindexRethrottle);
        LOGGER.info("updateByQueryRethrottle:{}.", updateByQueryRethrottle);
        LOGGER.info("deleteByQueryRethrottle:{}.", deleteByQueryRethrottle);
        return "ok";
    }

    public String mtermvectors() throws IOException {
        MultiTermVectorsRequest withOneWy = getWithOneWy();
        MultiTermVectorsRequest withTwoWay = getWithTwoWay();

        MultiTermVectorsResponse multiTermVectorsResponse1 = client.mtermvectors(withOneWy, RequestOptions.DEFAULT);
        MultiTermVectorsResponse multiTermVectorsResponse2 = client.mtermvectors(withTwoWay, RequestOptions.DEFAULT);
        LOGGER.info("multiTermVectorsResponse1:{}.", multiTermVectorsResponse1);
        LOGGER.info("multiTermVectorsResponse2:{}.", multiTermVectorsResponse2);
        return "ok";
    }

    private MultiTermVectorsRequest getWithOneWy() throws IOException {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();

        TermVectorsRequest tvrequest1 = new TermVectorsRequest("authors", "1");
        tvrequest1.setFields("user");
        request.add(tvrequest1);

        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject().field("user", "guest-user").endObject();
        TermVectorsRequest tvrequest2 = new TermVectorsRequest("authors", docBuilder);
        request.add(tvrequest2);
        return request;
    }

    private MultiTermVectorsRequest getWithTwoWay() {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest("authors", "fake_id");
        termVectorsRequest.setFields("user");
        String[] ids = {"1", "2"};
        MultiTermVectorsRequest request = new MultiTermVectorsRequest(ids, termVectorsRequest);
        return request;
    }

}
