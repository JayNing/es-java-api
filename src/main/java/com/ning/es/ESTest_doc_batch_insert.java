package com.ning.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.es.entity.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_doc_batch_insert {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 插入数据
        BulkRequest request = new BulkRequest();

        request.add(new IndexRequest().index("user").id("1002")
                .source(new ObjectMapper().writeValueAsString(new User("lisi","男",20)),XContentType.JSON));

        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON,"name","wangwu","sex","男","age",30));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","wangwu1","sex","男","age",40));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON,"name","wangwu2","sex","男","age",50));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON,"name","wangwu3","sex","男","age",30));
        request.add(new IndexRequest().index("user").id("1007").source(XContentType.JSON,"name","wangwu34","sex","男","age",20));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getItems());

        esClient.close();
    }
}
