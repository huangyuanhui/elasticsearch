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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
     * 升级版
     *
     * @param params
     * @return
     */
    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1：
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
    }

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
            buildAggregation(request);
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

    //@Override
    public Map<String, List<String>> filters_old() {
        try {
            // 1：构建请求
            SearchRequest request = new SearchRequest("hotel");
            // 2：准备DSL
            request.source().size(0);
            // 2：聚合
            buildAggregation(request);
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

    private void buildAggregation(SearchRequest request) {
        // 2.1：城市
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        // 2.2：品牌
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        // 2.3：星级
        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100)
        );
    }

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
