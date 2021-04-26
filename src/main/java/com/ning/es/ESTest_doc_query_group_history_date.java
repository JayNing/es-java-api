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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query_group_history_date {

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

        filterQueryBuilder.must(new TermQueryBuilder(GROUP_ID_KEYWORD, "25276,26174"));

        searchSourceBuilder.query(filterQueryBuilder);

        //todo 根据日期分组
        //createdDate不是keyword类型，不需要加createdDate.keyword
        TermsAggregationBuilder my = AggregationBuilders.terms("myDate").field("createdDate");
        //todo 分组后，每个相同groupId的结果
//        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits");
//        my.subAggregation(my_top_hits);

        //todo 进行分页和排序
//        BucketSortPipelineAggregationBuilder sortPipeline = new BucketSortPipelineAggregationBuilder("r_bucket_sort",null);
//        sortPipeline.from(0);
//        sortPipeline.size(6);
//        my.subAggregation(sortPipeline);
        //todo 添加size(100), 解决聚合查询结果桶数量默认只显示10条的问题
//        my.size(1000);
        my.size(Integer.MAX_VALUE);
        searchSourceBuilder.aggregation(my);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(0);
        searchSourceBuilder.trackTotalHits(true);

        searchRequest.source(searchSourceBuilder);

        SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);

        TreeSet<String> dateSet = new TreeSet<>();
        Terms agg = response.getAggregations().get("myDate");
        System.out.println("agg.getBuckets().size() : " + agg.getBuckets().size());
        for (Terms.Bucket bucket : agg.getBuckets()) {
            String dateString = bucket.getKeyAsString().replace("T00:00:00.000Z", "");
            dateSet.add(dateString);
        }

        System.out.println(dateSet);
        esClient.close();
    }
}
