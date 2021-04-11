package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_index_search {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        // 查询索引
//        GetIndexRequest request = new GetIndexRequest("user");
//        GetIndexResponse response = esClient.indices().get(request, RequestOptions.DEFAULT);
//        System.out.println(response.getAliases());
//        System.out.println(response.getMappings());
//        System.out.println(response.getSettings());

        // 删除索引
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        esClient.indices().delete(request, RequestOptions.DEFAULT);

        esClient.close();
    }
}
