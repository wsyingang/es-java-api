package com.es.api;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * ES常用的操作
 */
public class EsCommon {
    private Client client;
    public EsCommon(Client c){
        this.client=c;
    }
    /**
     * 创建一个Index
     * @param indexName
     */
    public boolean createIndex(String indexName){
        try {
            CreateIndexResponse indexResponse=this.client.admin()
                    .indices()
                    .prepareCreate(indexName)
                    .get();
            return indexResponse.isAcknowledged();
        }catch (ElasticsearchException e){
            e.printStackTrace();
            return false;
        }
    }
    /**
     * 给Index增加Mapping，就是增加type
     * @param index 索引的name
     * @param type 增加的type类型
     */
    public boolean addMapping(String index,String type){
        try {

            XContentBuilder builder= XContentFactory.jsonBuilder()
                    .startObject()
                            .field("properties")
                                .startObject()
                                    .field("name")
                                        .startObject()
                                            .field("index","not_analyzed")
                                            .field("type","string")
                                        .endObject()
                                    .field("age")
                                        .startObject()
                                            .field("index","not_analyzed")
                                            .field("type","integer")
                                        .endObject()
                                .endObject()
                    .endObject();
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
