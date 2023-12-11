package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Map;

/**
 * RestClient搜索
 */
@SpringBootTest
public class HotelSearchTest {

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

    /**
     * 1:全文检索查询，匹配所有：match_all
     *
     * @throws IOException
     */
    @Test
    public void testMatchAll() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        // 3：发送请求
        handlerResponse(request);
    }


    /**
     * 2:全文检索查询
     */
    /**
     * 2.1 match查询（单字段查询）
     *
     * @throws IOException
     */
    @Test
    public void testMatch() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.matchQuery("all", "如家")
        );
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 2.2 multi_match查询（多字段查询）
     *
     * @throws IOException
     */
    @Test
    public void testMultiMatch() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.multiMatchQuery(
                        "上海外滩如家",
                        "city", "name", "brand", "business")
        );
        // 3：发送请求
        handlerResponse(request);
    }


    /**
     * 3：精确查询
     */
    /**
     * 3.1：term词条查询
     */
    @Test
    public void testTerm() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.termQuery("city", "北京")
        );
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 3.2：range范围查询
     */
    @Test
    public void testRange() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.rangeQuery("price").gte(1000).lte(2000)
        );
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 4：geo地理位置查询
     */
    /**
     * 4.1：中心点附近查询 distance查询
     *
     * @throws IOException
     */
    @Test
    public void testGeoDistance() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.geoDistanceQuery("location")
                        .point(31.21, 121.5)
                        .distance(10, DistanceUnit.KILOMETERS)
        );
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 4.2：区域范围查询
     *
     * @throws IOException
     */
    @Test
    public void testGeoBoundingBox() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(
                QueryBuilders.geoBoundingBoxQuery("location")
                        .setCornersOGC(new GeoPoint(30.9, 121.5),
                                new GeoPoint(31.1, 121.7))
        );
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 5：组合查询 bool_query
     */
    @Test
    public void testBool() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL：查询核心API:QueryBuilders，所有查询条件都是由QueryBuilders构建的
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        // must
        queryBuilder.must(QueryBuilders.termQuery("city", "上海"));
        // should
        queryBuilder
                .should(QueryBuilders.termQuery("brand", "皇冠假日"))
                .should(QueryBuilders.termQuery("brand", "华美达"));
        // must_not
        queryBuilder.mustNot(QueryBuilders.rangeQuery("price").lte(500));
        // filter
        queryBuilder.filter(QueryBuilders.rangeQuery("score").gte(45));
        request.source().query(queryBuilder);
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 6：搜索结果处理（排序 分页 高亮）
     */
    /**
     * 6.1：排序
     *
     * @throws IOException
     */
    @Test
    public void testSort() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().sort("price", SortOrder.ASC);
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 6.2：分页
     *
     * @throws IOException
     */
    @Test
    public void testPage() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().sort("score", SortOrder.DESC).sort("price", SortOrder.ASC);
        request.source().from(0).size(5);
        // 3：发送请求
        handlerResponse(request);
    }

    /**
     * 6.3：高亮
     *
     * @throws IOException
     */
    @Test
    public void testHighLights() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 设置高亮
        request.source().highlighter(
                new HighlightBuilder()
                        .field("name").field("brand")
                        .requireFieldMatch(false)
        );
        // 3：发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4：解析结果
        SearchHits searchHits = response.getHits();
        System.out.println("共搜索到 " + searchHits.getTotalHits().value + " 条记录");
        // 默认10条
        for (SearchHit searchHit : searchHits) {
            // 4.1：得到source
            String json = searchHit.getSourceAsString();
            // 4.2：json的parse
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            // 4.3.1：获取高亮结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // 4.3.2：根据字段名获取高亮结果
                HighlightField nameHighlightField = highlightFields.get("name");
                // 4.3.3：获取高亮值
                String name = nameHighlightField.getFragments()[0].toString();
                // 4.3.4：覆盖非高亮结果
                if (!StringUtils.isEmpty(name)) {
                    hotelDoc.setName(name);
                }
                HighlightField brandHighlightField = highlightFields.get("name");
                String brand = brandHighlightField.getFragments()[0].toString();
                if (!StringUtils.isEmpty(brand)) {
                    hotelDoc.setBrand(brand);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }

    /**
     * 处理响应结果
     *
     * @param request
     * @throws IOException
     */
    private void handlerResponse(SearchRequest request) throws IOException {
        // 3：发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4：解析结果
        SearchHits searchHits = response.getHits();
        System.out.println("共搜索到 " + searchHits.getTotalHits().value + " 条记录");
        // 默认10条
        for (SearchHit searchHit : searchHits) {
            // 4.1：得到source
            String json = searchHit.getSourceAsString();
            // 4.2：json的parse
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            // 4.3.1：获取高亮结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // 4.3.2：根据字段名获取高亮结果
                HighlightField nameHighlightField = highlightFields.get("name");
                // 4.3.3：获取高亮值
                String name = nameHighlightField.getFragments()[0].toString();
                // 4.3.4：覆盖非高亮结果
                if (!StringUtils.isEmpty(name)) {
                    hotelDoc.setName(name);
                }
                HighlightField brandHighlightField = highlightFields.get("name");
                String brand = brandHighlightField.getFragments()[0].toString();
                if (!StringUtils.isEmpty(brand)) {
                    hotelDoc.setBrand(brand);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }
}
