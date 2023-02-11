package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    //定义线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Autowired
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺详情数据
     */
    @Override
    public Object queryGetById(Long id) {
        //解决缓存穿透
      //  Shop shop = queryWithPassThrougth(id);

        //使用互斥锁解决缓存击穿
      //Shop shop = queryWithMutex(id);

        //使用逻辑过期时间来解决缓存击穿
       // Shop shop = queryWithLogicalExpire(id);

        //解决缓存穿透
        Shop shop = cacheClient.queryWithPassThrougth(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //使用互斥锁解决缓存击穿
        //Shop shop = cacheClient.queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //使用逻辑过期时间来解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //使用逻辑过期时间来解决缓存击穿
    private Shop queryWithLogicalExpire(Long id){
        //1.从redis中查询数据
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，未命中返回空
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //3.命中，先把json反序列化为java对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //4判断缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
        //4.1 未过期，直接返回店铺信息
            return shop;
        }
        //4.2 已过期，需要缓存重建
        //5.缓存重建
        //5.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //5.2判断是否获取锁成功
        if (isLock){
            //5.2.1 获取锁成功后 做DoubleCheck；
        shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isBlank(shopJson)) {
                return null;
            }
             redisData = JSONUtil.toBean(shopJson, RedisData.class);
             shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
             expireTime = redisData.getExpireTime();
            if (expireTime.isAfter(LocalDateTime.now())) {
                return shop;
            }

        //5.3 是，重建缓存---》开启独立线程查询数据库
           CACHE_REBUILD_EXECUTOR.submit( ()->{
               try {
                   this.saveShop2Redis(id,20L);
               } catch (Exception e) {
                   throw new RuntimeException(e);
               }finally {
        //5.4释放锁
                   unlock(lockKey);
               }
           });
        }
        //6 返回过期得商铺信息
         return shop;


    }

  //使用互斥锁解决缓存击穿
    private Shop queryWithMutex(Long id) {
        //1.从redis中查询数据
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，命中侧返回商铺数据
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //2.1判断是否为空
        if (shopJson!=null) {
            return null;
        }
        //3.未命中
        //3.1尝试获取锁
        String lockKey = "lock:shop:"+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //3.2判断是否获取锁
            if (!isLock) {
            //3.3否，休眠一段时间
                Thread.sleep(50);
                //递归回溯
                return queryWithMutex(id);
            }
            //3.4是，
            // 3.4.1再次查询缓存。做DoubleCheck
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            if (shopJson!=null) {
                return null;
            }
            //3.4.2根据id查询数据库
            shop = getById(id);
            //4.判断是否存在
            if (shop == null) {
                //4.1不存在返回404
                //4.1将空值写入缓存中
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //4.2存在就将数据写入缓存中
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
        //5.释放锁
        unlock(lockKey);
        }
        //6.返回商铺数据
        return shop;
    }
    //释放锁
    private void unlock(String key) {
      stringRedisTemplate.delete(key);
    }

    //尝试获取锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //添加逻辑过期时间得缓存预热
    public void saveShop2Redis(Long id,Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    //解决缓存穿透
    public Shop queryWithPassThrougth(Long id){
        //1.从redis中查询数据
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，命中侧返回商铺数据
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //2.1判断是否为空
        if (shopJson!=null) {
            return null;
        }

        //3.未命中从数据库中根据id查询数据
        Shop shop = getById(id);
        //4.判断是否存在
        if (shop == null) {
            //4.1不存在返回404
            //4.1将空值写入缓存中
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //4.2存在就将数据写入缓存中
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //5.返回商铺数据
        return shop;
    }

    /**
     * 更新商铺信息
     * @param shop 商铺数据
     * @return 无
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
      //1.判断是否需要根据坐标查询
        if (x == null|| y==null){
            Page<Shop> shop = query()
                            .eq("type_id", typeId)
                            .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(shop.getRecords());
        }
        //需要查询redis
        //2.计算分页参数
        Integer from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        Integer end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis，按距离排序，分页。 结果为：shopId，distance
        //GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        //4.解析id
        if (results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from){
            //没有下一页结束
            return Result.ok(Collections.emptyList());
        }

        //4.1截取from -end 部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result ->{
            //4.2获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop :shops){
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //返回
        return Result.ok(shops);
    }
}
