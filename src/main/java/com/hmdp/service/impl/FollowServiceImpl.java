package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IUserService userService;

    //关注与取消
    @Override
    public Result follow(Long followUserId, boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follow:"+userId;
        if (isFollow){
           //关注
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess){
                //存入redis中
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            //取消关注 //sql delete from tb_follow where userId= userId and follow_user_id =?
            remove(new QueryWrapper<Follow>()
                    .eq("user_id",userId).eq("follow_user_id",followUserId));
            //移除redis
            stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
        }
        return Result.ok();
    }

    //判断是否已关注
    @Override
    public Result isFollow(Long followUserId) {
      //查询数据库，判断是否关注
        //1.select * from tb_follow where userId=userid and follow_user_id =?
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        // 2.判断
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long followUserId) {
        //获取当前用户的
        Long userId = UserHolder.getUser().getId();
        String key = "follow:"+userId;
        String key2="follow:"+followUserId;
        //1.求交集判断
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null||intersect.isEmpty()){
            //1.1无交集
            return Result.ok(Collections.emptyList());
        }
        //2.把set解析为Long类型的集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //3.查询用户
        List<UserDTO> userDTOS = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
