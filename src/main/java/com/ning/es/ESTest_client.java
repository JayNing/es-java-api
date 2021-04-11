package com.ning.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.net.InetAddress;

/**
 * @author ningjianjian
 * @Date 2021/4/11 3:47 下午
 * @Description
 */
public class ESTest_client {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient levelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", Integer.parseInt("9200"),"http"))
        );

        levelClient.close();
    }
}
