package cn.itcast.hotel.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 搜索返回值
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResult {
    private Long total; // 总条数
    private List<HotelDoc> hotels;  // 搜索结果
}
