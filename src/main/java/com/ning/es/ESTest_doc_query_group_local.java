package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query_group_local {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
//                RestClient.builder(new HttpHost("172.35.88.124", Integer.parseInt("9200"),"http"))
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 查询数据

        SearchRequest request = new SearchRequest();
        request.indices("im");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //根据年龄进行分组
        TermsAggregationBuilder my = AggregationBuilders.terms("my").field("age");
        TopHitsAggregationBuilder my_top_hits = AggregationBuilders.topHits("my_top_hits").from(0).size(1);
        my.subAggregation(my_top_hits);

        //todo 分组结果进行排序及分页
        my.subAggregation(new  BucketSortPipelineAggregationBuilder("seller_num_agg_sort",
                Arrays.asList(new FieldSortBuilder("doc_count").order(SortOrder.ASC))).from(0).size(3));

        searchSourceBuilder.aggregation(my);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(3);
        request.source(searchSourceBuilder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        System.out.println(response);

        Terms agg = response.getAggregations().get("my");
        for (Terms.Bucket bucket : agg.getBuckets()) {
            System.out.println(bucket.getKey());
            Aggregations aggregations = bucket.getAggregations();

            TopHits top = aggregations.get("my_top_hits");
            for (SearchHit searchHit : top.getHits()) {
                System.out.println(searchHit.getSourceAsMap());
            }
        }

        esClient.close();
    }
}
