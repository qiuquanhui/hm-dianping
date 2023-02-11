package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    //缓存列表
    @Override
    public Result queryList() {
        //1.从redis中查询数据
        String key="CACHE_SHOPTYPE_KEY";
        String shopTypeListJson = stringRedisTemplate.opsForValue().get(key);
        //2.命中侧返回
        if (StrUtil.isNotBlank(shopTypeListJson)) {
            //json数据转化为list对象
            List<ShopType> list = JSONObject.parseArray(shopTypeListJson, ShopType.class);
            return Result.ok(list);
        }
        //3.查询数据库
        List<ShopType> list = this.query().orderByAsc("sort").list();
        String typeListJson = JSONUtil.toJsonStr(list);
        //4.存入redis中
        stringRedisTemplate.opsForValue().set(key,typeListJson);
        //5.返回结果
        return Result.ok(list);
    }
}
