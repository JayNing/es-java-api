package com.ning.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ning.es.entity.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
public class ESTest_doc_insert {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 插入数据
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user1 = new User("zhangsan","男",30);
//        User user2 = new User("lisi","男",20);
//        User user3 = new User("wangwu","女",40);
//        User user4 = new User("zhangsan1","男",30);
//        User user5 = new User("wangwu44","女",50);
//        User user6 = new User("wangwu3","男",40);
        //向ES插入数据，必须将数据转换成JSON格式
        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(user1);
        request.source(s, XContentType.JSON);

        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());

        esClient.close();
    }
}
