package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * 接收前端搜索请求参数
 */
@Data
public class RequestParams {
    private String key;    // 搜索关键字
    private Integer page;   // 当前页码
    private Integer size;   // 每页大小
    private String sortBy;  // 排序字段

    private String brand;   // 品牌
    private String city;    // 城市
    private String starName;    // 星级
    private Integer minPrice;   // 最小值价格
    private Integer maxPrice;   // 最大值价格

    private String location;
}
