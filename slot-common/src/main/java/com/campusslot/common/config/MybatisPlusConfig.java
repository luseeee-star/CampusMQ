package com.campusslot.common.config;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.OptimisticLockerInnerInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MyBatis-Plus 配置。
 *
 * <p><b>踩坑记录</b>：3.5.9 起 jsqlparser 被拆成独立依赖，
 * {@code PaginationInnerInterceptor} 已不在核心包里，直接 import 会编译失败。
 * 需要分页（D2 管理端列表页）时补这个依赖：
 *
 * <pre>
 * &lt;dependency&gt;
 *   &lt;groupId&gt;com.baomidou&lt;/groupId&gt;
 *   &lt;artifactId&gt;mybatis-plus-jsqlparser&lt;/artifactId&gt;
 *   &lt;version&gt;3.5.9&lt;/version&gt;
 * &lt;/dependency&gt;
 * </pre>
 *
 * 然后把 {@code new PaginationInnerInterceptor(DbType.MYSQL)} 加进拦截器链。
 */
@Configuration
public class MybatisPlusConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        // 乐观锁：配合 UPDATE ... WHERE version = ? 防止并发覆盖
        interceptor.addInnerInterceptor(new OptimisticLockerInnerInterceptor());
        return interceptor;
    }
}
