package com.example.demo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Administrator on 2018/12/15.
 */
@Configuration
public class ESconfig {


    private static final Logger LOGGER = LoggerFactory.getLogger(ESconfig.class);

    /**
     * elk集群地址
     */
    @Value("${elasticsearch.ip}")
    private String hostName;
    /**
     * 端口
     */
    @Value("${elasticsearch.port}")
    private Integer port;
    /**
     * 集群名称
     */
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    /**
     * 连接池
     */
    @Value("${elasticsearch.pool}")
    private String poolSize;

    @Bean
    public RestHighLevelClient init123() {
        LOGGER.info("初始化开始。。。。。");
        RestHighLevelClient transportClient = null;
        try {
           // 配置信息
            transportClient = new RestHighLevelClient(RestClient.builder(new HttpHost(hostName, port, "http")));
        } catch (Exception e) {
            LOGGER.error("elasticsearch TransportClient create error!!!", e);
        }
        return transportClient;
    }






}
