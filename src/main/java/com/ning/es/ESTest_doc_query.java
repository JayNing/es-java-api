package com.ning.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.es.entity.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_query {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 查询数据

        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.termQuery("age",30));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").gte(20).lte(50));
        boolQuery.must(QueryBuilders.fuzzyQuery("name","wangwu").fuzziness(Fuzziness.TWO));

        searchSourceBuilder.query(boolQuery);


//        searchSourceBuilder.from(1);
//        searchSourceBuilder.size(2);

        request.source(searchSourceBuilder);




        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        System.out.println(response.getHits());
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        esClient.close();
    }
}
