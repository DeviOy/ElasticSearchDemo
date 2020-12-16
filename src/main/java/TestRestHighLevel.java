import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
/**
 * @Author: ylz
 * @ClassName: TestRestHighLevel
 * @Date: 2020/12/2 15:55
 * @company：北京天源迪科信息技术有限公司
 * @Description:
 */

public class TestRestHighLevel {
    private RestHighLevelClient client;

    @Before
    public void init(){
        RestClientBuilder restClientBuilder= RestClient.builder(
                new HttpHost("192.168.91.128",9200,"http"),
                new HttpHost("192.168.91.129",9200,"http"),
                new HttpHost("192.168.91.130",9200,"http"));
        this.client=new RestHighLevelClient(restClientBuilder);
    }

    @After
    public void after() throws IOException {
        this.client.close();
    }

    //新增文档，同步操作
    @Test
    public void testCreate() throws IOException {
        Map<String,Object> data=new HashMap<String, Object>();
        data.put("id",1009);
        data.put("name","highlevelEs");
        data.put("age",120);
        data.put("sex","无");

        IndexRequest indexRequest=new IndexRequest("haoke","userId").source(data);
        IndexResponse indexResponse =this.client.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("id->"+indexResponse.getId());
        System.out.println("index->"+indexResponse.getIndex());
        System.out.println("type->"+indexResponse.getType());
        System.out.println("version->"+indexResponse.getVersion());
        System.out.println("result->"+indexResponse.getResult());
        System.out.println("shardInfo->"+indexResponse.getShardInfo());
    }

    //新增文档，异步操作
    @Test
    public void testCreateAsync() throws InterruptedException {
        Map<String,Object> data=new HashMap<String, Object>();
        data.put("id",1008);
        data.put("name","async_create");
        data.put("age",23);
        data.put("sex","无");
        IndexRequest indexRequest=new IndexRequest("haoke","userId").source(data);

        this.client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("index->"+indexResponse.getIndex());
                System.out.println("type->"+indexResponse.getType());
                System.out.println("version->"+indexResponse.getVersion());
                System.out.println("result->"+indexResponse.getResult());
                System.out.println("shardinfo->"+indexResponse.getShardInfo());
                System.out.println("id->"+indexResponse.getId());
            }

            public void onFailure(Exception e) {

            }
        });
        System.out.println("ok");
        Thread.sleep(2000);
    }

    @Test
    public void testQuery() throws IOException {
        GetRequest getRequest=new GetRequest("haoke","userId","NzE_InYBrZrPSs7Y4qv7");

        //指定返回的字段
        String[] includes=new String[]{"id","name"};
        String[] excludes= Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext=new FetchSourceContext(true,includes,excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse response=this.client.get(getRequest,RequestOptions.DEFAULT);
        System.out.println("数据->"+response.getSourceAsString());
    }
    //判断是否存在
    @Test
    public void testExists() throws IOException {
        GetRequest getRequest=new GetRequest("haoke","userId","NzE_InYBrZrPSs7Y4qv7");
        //不返回的字段
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        boolean exists=this.client.exists(getRequest,RequestOptions.DEFAULT);
        System.out.println("exists"+exists);
    }
    //删除数据
    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest=new DeleteRequest("haoke","userId","1001");
        DeleteResponse deleteResponse=this.client.delete(deleteRequest,RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }
    //更新数据
    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest=new UpdateRequest("haoke","userId","1005");
        Map<String,Object> data=new HashMap<String, Object>();
        data.put("name","王老五");
        updateRequest.doc(data);
        UpdateResponse updateResponse=this.client.update(updateRequest,RequestOptions.DEFAULT);
        System.out.println("version->"+updateResponse.getVersion());

    }

    //测试搜索
    @Test
    public void testsearch() throws IOException {
        SearchRequest searchRequest=new SearchRequest("haoke");
        searchRequest.types("userId");

        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name","王老五"));
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse=this.client.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println("搜索到"+searchResponse.getHits().getTotalHits()+"条数据.");
        SearchHits hits=searchResponse.getHits();
        for (SearchHit hit:hits) {
            System.out.println(hit.getSourceAsString());
        }
    }




}
