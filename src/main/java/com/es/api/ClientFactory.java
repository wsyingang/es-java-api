package com.es.api;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ClientFactory {
    // 构造第一种NodeClient
    public static Client GetNodeClient(){
        Node esNode=nodeBuilder()
                .clusterName("es")
                .data(true)
                .node();
        Client client=esNode.client();
        return client;
    }
    // 构造TransportClient
    public static Client GetTransportClient(){
        Settings esSetting = settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .build();
        TransportClient transportClient = new TransportClient(esSetting);
        // 添加连接地址
        TransportAddress address = new InetSocketTransportAddress("127.0.0.1", 9300);
        transportClient.addTransportAddress(address);
        return transportClient;
    }
}
