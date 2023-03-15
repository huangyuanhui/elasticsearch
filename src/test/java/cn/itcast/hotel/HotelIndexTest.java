package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * 索引库操作
 */
@SpringBootTest
public class HotelIndexTest {

    private RestHighLevelClient client;

    @BeforeEach
    public void setUp() {
        // 初始化RestHighLevelClient
        this.client = new RestHighLevelClient(RestClient.builder(
             HttpHost.create("http://120.77.168.189:9200")
        ));
    }

    @AfterEach
    public void tearDown() throws IOException {
        // 释放资源
        this.client.close();
    }

    @Test
    public void testInit() {
        /**
         * 打印client信息，查看初始化是否成功：
         * client = org.elasticsearch.client.RestHighLevelClient@51e3d37e
         */
        System.out.println("client = " + client);
    }

    /**
     * 创建索引库
     * @throws IOException
     */
    @Test
    public void createHotelIndex() throws IOException {
        // 第1步：创建请求Request对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("hotel");
        // 第2步：准备请求的参数：DSL语句
        createIndexRequest.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 第3步：发送请求（第一个参数是Request对象，第二个参数一般走默认）
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引库
     * @throws IOException
     */
    @Test
    public void  testDeleteHotelIndex() throws IOException {
        // 1：创建请求Request对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("hotel");
        // 2：发送请求
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引库是否存在
     * @throws IOException
     */
    @Test
    public void testHotelIndexExist() throws IOException {
        // 1：创建请求Request对象
        GetIndexRequest getIndexRequest = new GetIndexRequest("hotel");
        // 2：发送请求
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);
    }

}
