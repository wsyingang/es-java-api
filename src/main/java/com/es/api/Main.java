package com.es.api;

public class Main {
    public static void main(String[] args) {
        EsCommon esClient=new EsCommon(ClientFactory.GetTransportClient());
        String index="person";
        String type="test";
    }
}
