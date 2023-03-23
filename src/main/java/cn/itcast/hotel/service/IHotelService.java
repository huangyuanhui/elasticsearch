package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {

    /**
     * 搜索
     * @param params
     * @return
     */
    PageResult search(RequestParams params);

    /**
     * 聚合
     * @param params
     * @return
     */
    Map<String, List<String>> filters(RequestParams params);


    /**
     * 聚合
     * @return
     */
    Map<String, List<String>> filters();
}
