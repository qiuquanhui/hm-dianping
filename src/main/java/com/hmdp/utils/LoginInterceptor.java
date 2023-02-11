package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 作者:灰爪哇
 * 时间:2023-01-29
 */
public class LoginInterceptor implements HandlerInterceptor {


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {


        //1.判断用户是否存在
        if (UserHolder.getUser() == null) {
            //1.2不存在就拦截
            response.setStatus(401);
            return false;
        }
        //2.放行
        return true;
    }
}
