package com.hmdp.utils;


import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 作者:灰爪哇
 * 时间:2023-02-03
 */
public class SimpleRedisLock implements Ilock{

    //线程标识id前缀
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"_";
   //锁前缀名
    private static final String KEY_PREFIX ="lock:";

   private StringRedisTemplate stringRedisTemplate;
  //锁名
   private String name;

   //调用Lua脚本得函数
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("Unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }


    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标示
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }

    //调用Lua脚本
    @Override
    public void unlock() {
      stringRedisTemplate.execute(
              UNLOCK_SCRIPT,       //lua脚本
              Collections.singletonList(KEY_PREFIX + name),  //keys
              ID_PREFIX + Thread.currentThread().getId()  //argv
              );
    }

    /**
    // 释放锁
    @Override
    public void unlock() {
        //判断锁是否是自己得
        //获取线程标示
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if (id.equals(threadId)){
        //通过del删除锁
        stringRedisTemplate.delete(KEY_PREFIX + name);
        }
    }
    */
}
