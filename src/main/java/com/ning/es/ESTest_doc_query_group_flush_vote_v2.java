package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query_group_flush_vote_v2 {

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

        //通用查询条件，非删除状态，而且必须我是接收者
        filterQueryBuilder.must(new TermQueryBuilder(IS_DELETED_KEYWORD, "0"));
        filterQueryBuilder.must(new TermsQueryBuilder(MSG_TYPE_KEYWORD, "100"));
        filterQueryBuilder.must(new TermsQueryBuilder("groupId.keyword", "840587314681397248"));
        filterQueryBuilder.must(new WildcardQueryBuilder("msgGroupId.keyword", "840594313776123904_*"));

        BoolQueryBuilder inQueryBuilder = new BoolQueryBuilder();
        inQueryBuilder.should(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "O26536"));
        inQueryBuilder.should(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "26174"));
        inQueryBuilder.should(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "24537"));
        inQueryBuilder.should(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "D26313"));

        filterQueryBuilder.must(inQueryBuilder);

        searchSourceBuilder.query(filterQueryBuilder);

        //todo 根据接收人分组
        TermsAggregationBuilder my = AggregationBuilders.terms("my").field(RECEIVER_ID_KEYWORD);
        //todo 分组后，每个相同群组的结果
//        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits");
        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits").size(100);
        my.subAggregation(my_top_hits);
        my.size(20000);

        searchSourceBuilder.aggregation(my);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(0);
        searchSourceBuilder.trackTotalHits(true);

        searchSourceBuilder.timeout(new TimeValue(TIME_OUT, TimeUnit.MILLISECONDS));
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);

        Map<String, Set<String>> resultMap = new HashMap<>();

        Terms agg = response.getAggregations().get("my");
        for (Terms.Bucket bucket : agg.getBuckets()) {
            String receiverId = (String) bucket.getKey();
            Set<String> msgIdSet = new HashSet<>();

            Aggregations aggregations = bucket.getAggregations();
            TopHits top = aggregations.get("my_top_hits");
            for (SearchHit searchHit : top.getHits()) {
                Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                Long msgId = (Long) sourceAsMap.get("msgId");
                msgIdSet.add(String.valueOf(msgId));
            }
            if (msgIdSet.size() > 0){
                resultMap.put(receiverId, msgIdSet);
            }
        }

        System.out.println(resultMap);

        System.out.println(convertMap(resultMap));

        esClient.close();
    }

    private static Map<String, Set<String>> convertMap(Map<String, Set<String>> map) {
        Map<String, Set<String>> resultMap = new HashMap<>();

        for (Map.Entry<String,Set<String>> entry : map.entrySet()){
            String receiverId = entry.getKey();
            Set<String> msgIdSet = entry.getValue();
            for (String msgId : msgIdSet) {
                Set<String> recIdSet = resultMap.get(msgId);
                if (recIdSet == null){
                    recIdSet = new HashSet<>();
                }
                recIdSet.add(receiverId);
                resultMap.put(msgId, recIdSet);
            }
        }
        return resultMap;
    }
}
