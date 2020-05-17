package com.es.api;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
public class EsNodeClient {
    public static void main(String[] args) {
        Node esNode=nodeBuilder()
                .clusterName("es")
                .data(true)
                .node();
        Client client=esNode.client();
    }
}
