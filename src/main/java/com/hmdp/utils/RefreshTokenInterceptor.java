package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * 作者:灰爪哇
 * 时间:2023-01-29
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    //因为这个不是spring管理的类，所以没有使用autowire自动录入
   private StringRedisTemplate stringRedisTemplate;

   public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate){
       this.stringRedisTemplate = stringRedisTemplate;
   }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
       //移除
        UserHolder.removeUser();
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取请求头
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return true;
        }
        //2.基于token获取redis用户
        String tokenKey = LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(tokenKey);
        //3.判断用户是否存在
        if (userMap.isEmpty()) {
            return true;
        }
        //4.将查询到的Hash数据转化为userDTO
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        //5.存在就把数据保存到ThreadLocal中
        UserHolder.saveUser(userDTO);
        //6.刷新token的有效期
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL, TimeUnit.MINUTES);
        //7.放行
        return true;

       //1.获取session
     //   HttpSession session = request.getSession();
        //2.获取用户
       // Object user = session.getAttribute("user");
        //3.判断用户是否存在
      //  if (user == null) {
            //3.1不存在就拦截
         //   response.setStatus(401);
          //  return false;
       // }
        //4.保存用户到threadlocal中
      //  UserHolder.saveUser((UserDTO) user);
        //5.放行
     //   return true;
    }
}
