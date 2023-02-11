package com.hmdp.utils;

/**
 * 作者:灰爪哇
 * 时间:2023-02-03
 */
public interface Ilock {
    //尝试获取锁
    boolean tryLock(long timeoutSec);
    //释放锁
    void unlock();
}
