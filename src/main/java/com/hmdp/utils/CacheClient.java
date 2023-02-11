package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * 作者:灰爪哇
 * 时间:2023-01-31
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //设置方法
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    //设置逻辑过期时间
    public void setWithLogicalExpire(String key, Object value,Long time ,TimeUnit unit){
        //设置逻辑时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    //解决缓存穿透
    public <R,ID> R queryWithPassThrougth(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID,R> dbFallBack,
            Long time,
            TimeUnit unit
    ){
        //1.从redis中查询数据
        String key = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，命中侧返回商铺数据
        if (StrUtil.isNotBlank(Json)) {
            R r = JSONUtil.toBean(Json, type);
            return r;
        }
        //2.1判断是否为空
        if (Json!=null) {
            return null;
        }

        //3.未命中从数据库中根据id查询数据
        R r = dbFallBack.apply(id);
        //4.判断是否存在
        if (r == null) {
            //4.1不存在返回404
            //4.1将空值写入缓存中
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //4.2存在就将数据写入缓存中
        this.set(key,r,time,unit);
        //5.返回商铺数据
        return r;
    }

    //使用逻辑过期时间来解决缓存击穿
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID,R> dbFallBack,
            Long time,
            TimeUnit unit
            ){
        //1.从redis中查询数据
        String key = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，未命中返回空
        if (StrUtil.isBlank(Json)) {
            return null;
        }
        //3.命中，先把json反序列化为java对象
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //4判断缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //4.1 未过期，直接返回店铺信息
            return r;
        }
        //4.2 已过期，需要缓存重建
        //5.缓存重建
        //5.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //5.2判断是否获取锁成功
        if (isLock){
            //5.2.1 获取锁成功后 做DoubleCheck；
            Json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isBlank(Json)) {
                return null;
            }
            redisData = JSONUtil.toBean(Json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            if (expireTime.isAfter(LocalDateTime.now())) {
                return r;
            }
        //5.3 是，重建缓存---》开启独立线程查询数据库
            CACHE_REBUILD_EXECUTOR.submit( ()->{
                try {
                    //查询数据库
                    R newR = dbFallBack.apply(id);
                    //重建缓存
                    this.setWithLogicalExpire(key,newR,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //5.4释放锁
                    unlock(lockKey);
                }
            });
        }
        //6 返回过期得商铺信息
        return r;


    }

    //使用互斥锁解决缓存击穿
    public <R,ID> R queryWithMutex(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID,R> dbFallBack,
            Long time,
            TimeUnit unit
            ) {
        //1.从redis中查询数据
        String key = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        //2.是否命中，命中侧返回商铺数据
        if (StrUtil.isNotBlank(Json)) {
            R r = JSONUtil.toBean(Json, type);
            return r;
        }
        //2.1判断是否为空
        if (Json!=null) {
            return null;
        }
        //3.未命中
        //3.1尝试获取锁
        String lockKey = LOCK_SHOP_KEY +id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            //3.2判断是否获取锁
            if (!isLock) {
                //3.3否，休眠一段时间
                Thread.sleep(50);
                //递归回溯
                return queryWithMutex(keyPrefix,id,type,dbFallBack,time,unit);
            }
            //3.4是，
            // 3.4.1再次查询缓存。做DoubleCheck
             Json = stringRedisTemplate.opsForValue().get(key);
            //2.是否命中，命中侧返回商铺数据
            if (StrUtil.isNotBlank(Json)) {
                 r = JSONUtil.toBean(Json, type);
                return r;
            }
            //2.1判断是否为空
            if (Json!=null) {
                return null;
            }

            //3.4.2根据id查询数据库
             r = dbFallBack.apply(id);
            //4.判断是否存在
            if (r == null) {
                //4.1不存在返回404
                //4.1将空值写入缓存中
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //4.2存在就将数据写入缓存中
           this.set(key,r,time,unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //5.释放锁
            unlock(lockKey);
        }
        //6.返回商铺数据
        return r;
    }
    //释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    //尝试获取锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);//不存在得话才能成功加入缓存
        return BooleanUtil.isTrue(flag);
    }

}
