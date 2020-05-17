package com.es.api;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ES常用的操作
 */
public class EsCommon {
    private Client client;

    public EsCommon(Client c) {
        this.client = c;
    }

    /**
     * 创建一个Index
     *
     * @param indexName
     */
    public boolean createIndex(String indexName) {
        try {
            CreateIndexResponse indexResponse = this.client.admin()
                    .indices()
                    .prepareCreate(indexName)
                    .get();
            return indexResponse.isAcknowledged();
        } catch (ElasticsearchException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 给Index增加Mapping，就是增加type
     *
     * @param index 索引的name
     * @param type  增加的type类型
     */
    public boolean addMapping(String index, String type) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("properties")
                    .startObject()
                    .field("name")
                    .startObject()
                    .field("index", "not_analyzed")
                    .field("type", "string")
                    .endObject()
                    .field("age")
                    .startObject()
                    .field("index", "not_analyzed")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println("get the json is " + builder.toString());
            PutMappingRequest mappingRequest = Requests.putMappingRequest(index).source(builder).type(type);
            this.client.admin().indices().putMapping(mappingRequest).actionGet();
            return true;
        } catch (ElasticsearchException e) {
            System.out.println("es some error ");
            e.printStackTrace();
        } catch (IOException e) {
            e.fillInStackTrace();
            System.out.println("io some error");
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName
     */
    public boolean deleteIndexByIndexName(String indexName) {
        DeleteIndexResponse deleteIndexResponse = this.client
                .admin()
                .indices()
                .prepareDelete(indexName)
                .get();
        return deleteIndexResponse.isAcknowledged();
    }

    /**
     * 创建一个文档
     *
     * @param index index
     * @param type  type
     */
    public boolean createDoc(String index, String type, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name", "yyy")
                    .field("age", 12)
                    .endObject();
            IndexResponse indexResponse = this.client
                    .prepareIndex()
                    .setIndex(index)
                    .setType(type)
                    .setId(id)
                    .setSource(builder.toString())
                    .get();
            return indexResponse.isCreated();
        } catch (ElasticsearchException e) {
            System.out.println("es some error" + e);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("io some error");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 更新文档
     *
     * @param index
     * @param type
     * @param id
     */
    public boolean updateDoc(String index, String type, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name", "lll")
                    .field("age", 15)
                    .endObject();
            UpdateResponse updateResponse = this.client
                    .prepareUpdate()
                    .setIndex(index)
                    .setType(type)
                    .setId(id)
                    .setDoc(builder.toString())
                    .get();
            return updateResponse.isCreated();
        } catch (ElasticsearchException e) {
            System.out.println("es some error");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("io some error");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除Doc
     *
     * @param index
     * @param type
     * @param id
     */
    public boolean deleteDocById(String index, String type, String id) {
        DeleteResponse deleteResponse = this.client
                .prepareDelete()
                .setIndex(index)
                .setType(type)
                .setId(id)
                .get();
        return deleteResponse.isFound();
    }

    /**
     * 根据查询条件删除文档
     *
     * @param index
     * @param type
     */
    public boolean deleteByQueryPara(String index, String type) {
        try {
            TermQueryBuilder queryBuilder = QueryBuilders.termQuery("name", "ll");
            DeleteByQueryResponse deleteByQueryResponses = this.client
                    .prepareDeleteByQuery()
                    .setTypes(type)
                    .setQuery(queryBuilder)
                    .get();
            return true;
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 使用Filter查询数据
     *
     * @param index 数据的索引name
     * @param type  数据所在的type
     */
    public List<String> queryByFilter(String index, String type) {
        FilterBuilder filterBuilder = FilterBuilders.termFilter("name", "yy");
        SearchResponse searchResponse = this.client
                .prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setPostFilter(filterBuilder)
                .get();
        List<String> docList = new ArrayList<String>();
        SearchHits searchHits = searchResponse.getHits();
        for (SearchHit hit : searchHits) {
            docList.add(hit.toString());
        }
        return docList;
    }

    /**
     * 聚合查询
     *
     * @param index
     * @param type
     */
    public double getMinAge(String index, String type) {
        SearchResponse response = this.client
                .prepareSearch(index)
                .addAggregation(AggregationBuilders.min("min").field("age"))
                .get();
        InternalMin min = response.getAggregations().get("min");
        return min.getValue();
    }
}

