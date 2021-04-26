package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
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

import java.io.IOException;
import java.util.List;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query_group_v2 {

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
        filterQueryBuilder.must(new TermQueryBuilder(RECEIVER_ID_KEYWORD, "26174"));

        //todo senderIdList
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(new TermQueryBuilder(SENDER_ID_KEYWORD, "24537"));
        boolQueryBuilder.should(new TermQueryBuilder(SENDER_ID_KEYWORD, "26174"));

        filterQueryBuilder.must(boolQueryBuilder);

//        filterQueryBuilder.must(new TermQueryBuilder(SENDER_ID_KEYWORD, "24535"));
//        filterQueryBuilder.must(new TermQueryBuilder(SENDER_ID_KEYWORD, "26174"));

//        filterQueryBuilder.must(new FuzzyQueryBuilder(INDEX_FIELD_KEYWORD, "世界"));
        //todo keyword查询
        filterQueryBuilder.must(new WildcardQueryBuilder(INDEX_FIELD_KEYWORD, "*5*"));

        //todo 时间范围查询
        long start =  0;
        long end = Long.MAX_VALUE;
        filterQueryBuilder.must(new RangeQueryBuilder(TIME).from(start).to(end).includeLower(true).includeUpper(false));

        searchSourceBuilder.query(filterQueryBuilder);

        //todo 根据关键字分组
        TermsAggregationBuilder my = AggregationBuilders.terms("my").field("index.keyword");
        TermsAggregationBuilder myGroupId = AggregationBuilders.terms("myGroupId").field("groupId.keyword");
//        TermsAggregationBuilder myTime = AggregationBuilders.terms("myTime").field("time.keyword");
//        myGroupId.subAggregation(myTime);
        //todo 分组后，每个相同groupId的结果
        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits");
        myGroupId.subAggregation(my_top_hits);
        my.subAggregation(myGroupId);

//        MaxAggregationBuilder maxTime = AggregationBuilders.max("maxTime").field("time");
//        my.subAggregation(maxTime);
        //todo 分组后，每个相同关键字的结果只显示一条
//        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits");
//        myGroupId.subAggregation(my_top_hits);

        //todo 进行分页和排序
        BucketSortPipelineAggregationBuilder sortPipeline = new BucketSortPipelineAggregationBuilder("r_bucket_sort",null);
        sortPipeline.from(0);
        sortPipeline.size(6);
        my.subAggregation(sortPipeline);
        //todo 添加size(100), 解决聚合查询结果桶数量默认只显示10条的问题
        my.size(Integer.MAX_VALUE);
        searchSourceBuilder.aggregation(my);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(0);

        searchRequest.source(searchSourceBuilder);

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);

        Terms agg = response.getAggregations().get("my");
        System.out.println("agg.getBuckets().size() : " + agg.getBuckets().size());
        for (Terms.Bucket bucket : agg.getBuckets()) {
            Aggregations aggregations = bucket.getAggregations();
            Terms group = aggregations.get("myGroupId");
            List<? extends Terms.Bucket> groupBuckets = group.getBuckets();
            for (Terms.Bucket groupBucket : groupBuckets) {
                Aggregations bucketAggregations = groupBucket.getAggregations();
                TopHits topHits = bucketAggregations.get("my_top_hits");
                for (SearchHit searchHit : topHits.getHits()) {
                    System.out.println(searchHit.getSourceAsString());
                }
            }
        }

        esClient.close();
    }
}
