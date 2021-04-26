package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query_group_querynewest {

    private static final String MSD_ID = "msgId";
    private static final String MSD_ID_KEYWORD = "msgId.keyword";
    private static final String GROUP_ID = "groupId";
    private static final String GROUP_ID_KEYWORD = "groupId.keyword";
    private static final String RECEIVER_ID = "receiverId";
    private static final String RECEIVER_ID_KEYWORD = "receiverId.keyword";
    private static final String SENDER_ID_KEYWORD = "senderId.keyword";
    private static final String MSG_TYPE = "msgType";
    private static final String MSG_TYPE_KEYWORD = "msgType.keyword";
    private static final String TIME = "time";
    private static final String TIME_KEYWORD = "time.keyword";
    private static final String IS_DELETED = "isDeleted";
    private static final String IS_DELETED_KEYWORD = "isDeleted.keyword";
    private static final String MODIFIED_TIME = "modifiedTime";
    private static final String MODIFIED_TIME_KEYWORD = "modifiedTime.keyword";
    private static final String INDEX_FIELD = "index";
    private static final String INDEX_FIELD_KEYWORD = "index.keyword";

    private static final String CONFLICT = "proceed";
    private static final int TIME_OUT = 5000;
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("172.35.88.124", Integer.parseInt("9200"),"http"))
//                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 查询数据
        SearchRequest searchRequest = new SearchRequest("im");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder filterQueryBuilder = new BoolQueryBuilder();

        filterQueryBuilder.must(new TermQueryBuilder(IS_DELETED_KEYWORD, 0));
        filterQueryBuilder.must(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "26174"));

        String groupId = "24537,26174";
        filterQueryBuilder.must(new TermQueryBuilder(GROUP_ID_KEYWORD, groupId));

        searchSourceBuilder.query(filterQueryBuilder);
        searchSourceBuilder.sort(new FieldSortBuilder(MSD_ID).order(SortOrder.DESC).unmappedType("long"));

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);
        searchSourceBuilder.timeout(new TimeValue(TIME_OUT, TimeUnit.MILLISECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);

        esClient.close();
    }
}
