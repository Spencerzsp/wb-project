package com.wbbigdata.es;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;

import java.io.IOException;

public class ESClientFactory {

    private static RestHighLevelClient restHighLevelClient;
    private static MainResponse response;


    public static RestHighLevelClient getRestHighLevelClient(){
        try {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("wbbigdata00", 9200, "http")
                    )
            );
            response = restHighLevelClient.info(RequestOptions.DEFAULT);
            ClusterName clusterName = response.getClusterName();
            Version version = response.getVersion();
            String nodeName = response.getNodeName();
            System.out.println(clusterName + "\t" + version + "\t" + nodeName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return restHighLevelClient;
    }

    /**
     * 关闭client连接
     */
    public static void closeClient(){
        if (restHighLevelClient != null){
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        getRestHighLevelClient();

        closeClient();
    }
}
