package com.wbbigdata.es;

import com.alibaba.fastjson.JSONObject;
import com.wbbigdata.bean.User;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class ESApp {

    /**
     * 创建索引
     * @param indexName
     * @return
     * @throws Exception
     */
    public static CreateIndexResponse createIndexRequest(String indexName) throws Exception {

        RestHighLevelClient client = ESClientFactory.getRestHighLevelClient();

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // 创建索引的主分片和副分片的个数
        request.settings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 2)
        );

        // 创建索引时，创建文档类型映射
        request.mapping("test_type",
                "{\n" +
                        "      \"test_type\": {\n" +
                        "        \"properties\": {\n" +
                        "          \"addr\": {\n" +
                        "            \"type\": \"text\",\n" +
                        "            \"fields\": {\n" +
                        "              \"keyword\": {\n" +
                        "                \"type\": \"keyword\",\n" +
                        "                \"ignore_above\": 256\n" +
                        "              }\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"name\": {\n" +
                        "            \"type\": \"text\",\n" +
                        "            \"fields\": {\n" +
                        "              \"keyword\": {\n" +
                        "                \"type\": \"keyword\",\n" +
                        "                \"ignore_above\": 256\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
        request.alias(
                new Alias("test_index_alias")
        );

        request.timeout(TimeValue.timeValueMinutes(2));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.waitForActiveShards(2);

        RequestOptions options = RequestOptions.DEFAULT;
        // 同步创建索引
        CreateIndexResponse createIndexResponse = client.indices().create(request, options);

        // 异步创建索引
//        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
//            @Override
//            public void onResponse(CreateIndexResponse createIndexResponse) {
//
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                e.printStackTrace();
//            }
//        };
//        client.indices().createAsync(request, options, listener);

        //指示是否所有节点都已确认请求
        boolean acknowledged = createIndexResponse.isAcknowledged();

        //指示是否在超时之前为索引中的每个分片启动了必需的分片副本数
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

        System.out.println(acknowledged + "\t" + shardsAcknowledged);
        System.out.println(indexName + " 索引创建成功！");

        return createIndexResponse;
    }

    /**
     * 删除索引
     * @param indexName
     * @return
     * @throws Exception
     */
    public static DeleteIndexResponse deleteIndex(String indexName) throws Exception {
        RestHighLevelClient client = ESClientFactory.getRestHighLevelClient();
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);

        RequestOptions options = RequestOptions.DEFAULT;
        DeleteIndexResponse response = client.indices().delete(request, options);

        System.out.println(indexName + " 索引删除成功！");
        return response;
    }

    /**
     * 根据指定文档id查询es数据
     * @param id
     * @return
     */
    public static SearchResponse selectById(String id){
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
//            TermQueryBuilder ids = QueryBuilders.termQuery("id", id);
//            boolQueryBuilder.must(ids);
//            searchSourceBuilder.size(1);
//            searchSourceBuilder.query(boolQueryBuilder);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("student");
            SearchRequest request = searchRequest.source(searchSourceBuilder);

            RestHighLevelClient client = ESClientFactory.getRestHighLevelClient();
            RequestOptions options = RequestOptions.DEFAULT;
            SearchResponse response = client.search(request, options);
            SearchHits hits = response.getHits();
            for(SearchHit hit: hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
            return response;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询全部数据
     * @param indexName
     * @return
     */
    public static ArrayList<User> selectAll(String indexName){
        ArrayList<User> users = new ArrayList<User>();
        RestHighLevelClient client = ESClientFactory.getRestHighLevelClient();
        RequestOptions options = RequestOptions.DEFAULT;
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            searchSourceBuilder.query(matchAllQueryBuilder);
            SearchRequest request = new SearchRequest().source(searchSourceBuilder);
            SearchResponse response = client.search(request, options);
            SearchHits hits = response.getHits();

            for (SearchHit hit: hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
                User user = JSONObject.parseObject(sourceAsString, User.class);
                users.add(user);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return users;
    }
    public static void main(String[] args) throws Exception {

//        createIndexRequest("test_index");
        selectById("曹操");
//        deleteIndex("person");
        ESClientFactory.closeClient();
    }

}
