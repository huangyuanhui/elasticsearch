package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Resource
    private RestHighLevelClient client;

    /**
     * 搜索进阶版2：广告指定 function_score
     *
     * @param params
     * @return
     */
    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1：准备Request
            SearchRequest request = new SearchRequest("hotel");

            // 2：准备DSL
            // 2.1：query
            buildBasicQuery(request, params);
            // 2.2：分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);
            // 2.3：排序
            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                request.source().sort(
                        SortBuilders
                                .geoDistanceSort("location", new GeoPoint(location))
                                .order(SortOrder.ASC)
                                .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            // 4：解析响应结果
            return handleResponse(response);
        } catch (IOException e) {
            // 抛出一个运行时异常
            throw new RuntimeException(e);
        }
    }

    /**
     * 搜索进阶版2：排序和地理查询
     *
     * @param params
     * @return
     */
    /*@Override
    public PageResult search(RequestParams params) {
        try {
            // 1：准备Request
            SearchRequest request = new SearchRequest("hotel");

            // 2：准备DSL
            // 2.1：query
            buildBasicQuery(request, params);
            // 2.2：分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);
            // 2.3：排序
            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                request.source().sort(
                        SortBuilders
                                .geoDistanceSort("location", new GeoPoint(location))
                                .order(SortOrder.ASC)
                                .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            // 4：解析响应结果
            return handleResponse(response);
        } catch (IOException e) {
            // 抛出一个运行时异常
            throw new RuntimeException(e);
        }
    }*/

    /**
     * 搜索进阶版1：条件过滤
     *
     * @param params
     * @return
     */
    /*@Override
    public PageResult search(RequestParams params) {
        try {
            // 1：准备Request
            SearchRequest request = new SearchRequest("hotel");

            // 2：准备DSL
            // 2.1：query
            buildBasicQuery(request, params);
            // 2.2：分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);

            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            // 4：解析响应结果
            return handleResponse(response);
        } catch (IOException e) {
            // 抛出一个运行时异常
            throw new RuntimeException(e);
        }
    }*/

    /**
     * 构建查询条件
     *
     * @param params
     */
    private void buildBasicQuery(SearchRequest request, RequestParams params) {
        // 构建BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 搜索关键字的搜索：用must
        String searchKey = params.getKey();
        if (searchKey == null || "".equals(searchKey)) {
            // 关键字为空，查询所有match_all
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            // 关键字不为空，全文检索查询：match单字段查询
            boolQuery.must(QueryBuilders.matchQuery("all", searchKey));
        }
        // 条件过滤搜索：用filter
        // 城市条件
        String city = params.getCity();
        if (city != null && !"".equals(city)) {
            // term查询
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        // 品牌条件
        String brand = params.getBrand();
        if (brand != null && !"".equals(brand)) {
            // term查询
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        // 星级条件
        String star = params.getStarName();
        if (star != null && !"".equals(star)) {
            // term查询
            boolQuery.filter(QueryBuilders.termQuery("starName", star));
        }
        // 价格条件
        Integer minPrice = params.getMinPrice();
        Integer maxPrice = params.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price").gte(minPrice).lte(maxPrice));
        }

        // 算分控制（以上的就是原始查询）
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询，相关性算分的查询
                        boolQuery,
                        // function_score数组，算分函数数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中一个unction_score函数，算分函数
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件，满足条件的才会去改变相关性打分
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算法
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQuery);
    }

    /**
     * 搜索：基本搜索和分页
     *
     * @param params
     * @return
     */
    /*@Override
    public PageResult search(RequestParams params) {
        try {
            // 1：准备Request
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            // 2.1：query
            String searchKey = params.getKey();
            if (searchKey == null || "".equals(searchKey)) {
                // 查询所有match_all
                request.source().query(QueryBuilders.matchAllQuery());
            } else {
                // 全文检索查询：match单字段查询
                request.source().query(QueryBuilders.matchQuery("all", searchKey));
            }
            // 2.2：分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);
            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4：解析响应结果
            return handleResponse(response);
        } catch (IOException e) {
            // 抛出一个运行时异常
            throw new RuntimeException(e);
        }
    }*/

    /**
     * 解析响应结果
     *
     * @param response
     * @return
     */
    private PageResult handleResponse(SearchResponse response) {
        // 4：解析响应
        SearchHits searchHits = response.getHits();
        // 4.1：获取总条数
        long total = searchHits.getTotalHits().value;
        // 4.2：文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3：遍历
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            // 获取文档source
            String json = hit.getSourceAsString();
            // 反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                hotelDoc.setDistance(sortValues[0]);
            }
            hotels.add(hotelDoc);
        }
        // 4.4：封装返回
        return new PageResult(total, hotels);
    }

    /**
     * 升级版
     *
     * @param params
     * @return
     */
    /*@Override
    public PageResult search(RequestParams params) {
        try {
            // 1: request请求
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            // 2.1：query
            buildBasicQuery(params, request);
            // 2.2：排序
            // 2.2.1：排序字段
            if (!StringUtils.isEmpty(params.getSortBy()) && !"default".equals(params.getSortBy())) {
                request.source().sort(params.getSortBy(), SortOrder.DESC);
            }
            // 2.2.2：距离排序
            if (!StringUtils.isEmpty(params.getLocation())) {
                request.source().sort(
                        SortBuilders.geoDistanceSort(
                                "location",
                                new GeoPoint(params.getLocation())
                        ).order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS)
                );
            }
            // 2.3：分页
            request.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());
            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4：解析响应
            SearchHits hits = response.getHits();
            List<HotelDoc> hotels = new ArrayList<>();
            for (SearchHit hit : hits) {
                System.out.println("hit = " + hit);
                HotelDoc hotelDoc = JSON.parseObject(hit.getSourceAsString(), HotelDoc.class);
                // 获取排序值
                Object[] sortValues = hit.getSortValues();
                if (sortValues.length > 1) {
                    hotelDoc.setDistance(sortValues[1]);
                }
                hotels.add(hotelDoc);
            }
            return new PageResult(hits.getTotalHits().value, hotels);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            // 1：构建请求
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            request.source().size(0);
            // 2.1：query
            buildBasicQuery(params, request);
            // 2.2：聚合
            buildAggregations(request);
            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4：解析结果
            Map<String, List<String>> resultMap = new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            // 4.1：根据城市名称，获取城市聚合结果
            List<String> cities = getAggByName(aggregations, "cityAgg");
            resultMap.put("city", cities);
            // 4.2：根据品牌名称，获取品牌聚合结果
            List<String> brands = getAggByName(aggregations, "brandAgg");
            resultMap.put("brand", brands);
            // 4.3：根据星级名称，获取星级聚合结果
            List<String> starNames = getAggByName(aggregations, "starAgg");
            resultMap.put("starName", starNames);
            return resultMap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters() {
        try {
            // 1：构建请求
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            request.source().size(0);
            // 2.1：聚合
            buildAggregations(request);
            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4：解析结果
            Map<String, List<String>> resultMap = new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            // 4.1：城市聚合
            List<String> cities = getAggByName(aggregations, "cityAgg");
            resultMap.put("city", cities);
            // 4.2：品牌聚合
            List<String> brands = getAggByName(aggregations, "brandAgg");
            resultMap.put("brand", brands);
            // 4.3：星级聚合
            List<String> starNames = getAggByName(aggregations, "starAgg");
            resultMap.put("starName", starNames);
            return resultMap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构建聚合
     * @param request
     */
    private void buildAggregations(SearchRequest request) {
        // 2.1：城市聚合
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        // 2.2：品牌聚合
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        // 2.3：星级聚合
        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                // 要加keyword，索引库字段好像不支持驼峰？
                .field("starName.keyword")
                .size(100)
        );
    }

    /**
     * 解析聚合结果
     * @param aggregations
     * @param aggName
     * @return
     */
    private List<String> getAggByName(Aggregations aggregations, String aggName) {
        Terms aggregation = aggregations.get(aggName);
        List<? extends Terms.Bucket> aggregationBuckets = aggregation.getBuckets();
        List<String> list = new ArrayList<>();
        for (Terms.Bucket bucket : aggregationBuckets) {
            list.add(bucket.getKeyAsString());
        }
        return list;
    }

    /**
     * 构建查询条件
     *
     * @param request
     * @param params
     */
    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        // 2.1.1：构建BooleanQuery
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 2.1.2：关键字搜索
        String key = params.getKey();
        if (StringUtils.isEmpty(key)) {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        } else {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", key));
        }
        // 2.1.3：条件过滤
        // 2.1.3.1：城市条件
        if (!StringUtils.isEmpty(params.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        // 2.1.3.2：品牌条件
        if (!StringUtils.isEmpty(params.getBrand())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        // 2.1.3.3：星级条件
        if (!StringUtils.isEmpty(params.getStarName())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        // 2.1.3.4：价格条件
        if (params.getMinPrice() != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()));
        }
        if (params.getMaxPrice() != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(params.getMaxPrice()));
        }

        // 2.2：打分函数，算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        //原始查询，相关性算分的查询
                        boolQueryBuilder,
                        // function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中一个function score元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(5)
                                )
                        });
        request.source().query(functionScoreQueryBuilder);
    }


    //@Override
    public PageResult search_Old(RequestParams params) {
        try {
            // 1: request请求
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            // 2.1：query
            String key = params.getKey();
            if (StringUtils.isEmpty(key)) {
                request.source().query(QueryBuilders.matchAllQuery());
            } else {
                request.source().query(QueryBuilders.matchQuery("all", key));
            }
            // 2.2：排序
            if (!StringUtils.isEmpty(params.getSortBy()) && !"default".equals(params.getSortBy())) {
                request.source().sort(params.getSortBy(), SortOrder.DESC);
            }
            // 2.3：分页
            request.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());
            // 3：发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4：解析响应
            SearchHits hits = response.getHits();
            List<HotelDoc> hotels = new ArrayList<>();
            for (SearchHit hit : hits) {
                HotelDoc hotelDoc = JSON.parseObject(hit.getSourceAsString(), HotelDoc.class);
                hotels.add(hotelDoc);
            }
            return new PageResult(hits.getTotalHits().value, hotels);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
