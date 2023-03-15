package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 文档操作
 */
@SpringBootTest
public class HotelDocumentTest {

    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @BeforeEach
    public void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://120.77.168.189:9200")
        ));
    }

    @AfterEach
    public void tearDown() throws IOException {
        this.client.close();
    }

    @Test
    public void testAddDocument() throws IOException {
        Hotel hotel = hotelService.getById(61083);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 1：准备request对象（注意，索引库中对id的要求都是keyword，即要是字符串，所以要转换）
        IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2：准备json文档
        indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        /*
         * 3：发送请求，这里与索引库发请求的代码很接近，差别是此处不需要client.indices()，只是用client就行
         * ，因为indices是操作索引库，这里我们操作的是文档，直接client.index(xxx,xxx)就行，index代表的
         * 就是创建倒排索引的意思，添加文档不就是创建倒排索引吗
         *
         */
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testGetDocumentById() throws IOException {
        // 1：准备request对象
        GetRequest getRequest = new GetRequest("hotel", "61083");
        // 2：发送请求
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        // 3：解析响应结果数据
        String source = getResponse.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);
        System.out.println("hotelDoc = " + hotelDoc);
    }

    @Test
    public void testUpdateDocumentById() throws IOException {
        // 1：准备request对象
        UpdateRequest updateRequest = new UpdateRequest("hotel", "61083");
        // 2:准备参数，每两个参数为一对key value，需要更新什么字段数据就写什么，不需要全部字段都写
        updateRequest.doc(
                "price", 988,
                "starName", "四钻"
        );
        // 3：发送请求
        client.update(updateRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteDocumentById() throws IOException {
        // 1：准备request对象：根据id删除
        DeleteRequest deleteRequest = new DeleteRequest("hotel", "61083");
        // 2：发送请求
        client.delete(deleteRequest, RequestOptions.DEFAULT);
    }


    /**
     * 批量添加文档
     */
    @Test
    public void testPatchAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        /*
        IPage<Hotel> page = new Page<>(1, 10);
        hotelService.page(page);
        List<Hotel> hotels = page.getRecords();
        */

        List<Hotel> hotels = hotelService.list();
        hotels.forEach(data -> {
            HotelDoc hotelDoc = new HotelDoc(data);
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
