import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: ylz
 * @ClassName: TestESREST
 * @Date: 2020/12/2 14:28
 * @company：北京天源迪科信息技术有限公司
 * @Description:
 */
public class TestESREST {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RestClient restClient;

    @Before
    public void init() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("192.168.91.128", 9200, "http"),
                new HttpHost("192.168.91.129", 9200, "http"),
                new HttpHost("192.168.91.130", 9200, "http"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                System.out.println("出错了 ->" + node);
            }
        });
        this.restClient = restClientBuilder.build();

    }

    @After
    public void after() throws IOException {
        restClient.close();
    }

    //查询集群状态
    @Test
    public void testGetInfo() throws IOException {
        Request request = new Request("GET", "/_cluster/state");
        request.addParameter("pretty", "true");
        Response response = this.restClient.performRequest(request);

        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    //新增数据
    @Test
    public void testCreateData() throws IOException {
        Request request = new Request("POST", "/haoke/userId");

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("id",1006);
        data.put("name","esdemo");
        data.put("age",100);
        data.put("sex","无");

        request.setJsonEntity(MAPPER.writeValueAsString(data));
        Response response=this.restClient.performRequest(request);

        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));

    }

    //根据id查询数据
    @Test
    public void testQueryData() throws IOException {
        Request request=new Request("GET","/haoke/userId/1001");

        Response response=this.restClient.performRequest(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    //搜索数据
    @Test
    public void testSearchData() throws IOException {
        Request request=new Request("POST","/haoke/userId/_search");
        String searchJson="{\"query\":{\"match\":{\"name\":\"张三\"}}}";
        request.setJsonEntity(searchJson);
        request.addParameter("pretty","true");

        Response response=this.restClient.performRequest(request);

        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

}
