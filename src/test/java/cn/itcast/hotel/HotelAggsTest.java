package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * elastic高级（数据聚合 自动补全 数据同步 集群）
 */
@SpringBootTest
public class HotelAggsTest {

    private RestHighLevelClient client;

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
    public void testAggregation() throws IOException {
        // 1：准备request
        SearchRequest searchRequest = new SearchRequest("hotel");
        // 2：准备DSL
        // 2.1：设置size
        searchRequest.source().size(0);
        // 2.2：聚合
        searchRequest.source().aggregation(
                AggregationBuilders
                        .terms("brandAgg")
                        .field("brand")
                        .size(20)
        );
        // 3：发送请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 4：解析结果
        // 4.1：获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        // 4.2：根据名称获取具体聚合结果
        Terms brandTerms = aggregations.get("brandAgg");
        // 4.3：获取桶
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        // 4.4：便利
        for (Terms.Bucket bucket : buckets) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
            System.out.println("bucket.getDocCount() = " + bucket.getDocCount());
        }
    }

}
