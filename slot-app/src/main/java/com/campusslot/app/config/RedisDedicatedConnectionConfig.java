package com.campusslot.app.config;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * D7 第 0 步的诊断开关：让 Redis 连接池真正被用上。
 *
 * <p><b>为什么需要这段代码，而不是一个配置项。</b>
 * Spring Boot 3.3 的 {@code spring.data.redis.lettuce.pool.*} 只有在 classpath 上
 * 存在 {@code commons-pool2} 时才会被读取（{@code @ConditionalOnClass(GenericObjectPool)}），
 * 否则静默忽略。补上依赖之后还有第二道坎：{@link LettuceConnectionFactory} 的
 * {@code shareNativeConnection} 默认是 {@code true}，所有命令仍然走同一条共享连接，
 * 池子建了但不用。而这个开关<b>Boot 根本没有暴露成配置项</b>
 * （{@code spring-configuration-metadata.json} 里只有 pool.*、cluster.*、shutdown-timeout），
 * 传命令行参数不会报错，只会被当成未知键丢掉 —— 同一类"配了但没生效"。
 *
 * <p><b>为什么用 BeanPostProcessor。</b>
 * {@code setShareNativeConnection} 必须在 {@code afterPropertiesSet()} 之前调用，
 * 所以只能拦在初始化前；写成 {@code @Bean} 的静态方法可以保证它在容器早期就被注册。
 *
 * <p><b>默认关闭。</b>这是测量工具，不是功能。它把"一条共享连接"变成"最多 64 条池化连接"，
 * 会改变压测数字的含义：开着跑出来的吞吐不能和 D5 的历史曲线直接比。
 * 只有在做"6.7k 是不是连接画的线"这个对照实验时才打开。
 */
@Configuration
@ConditionalOnProperty(name = "campusslot.redis.dedicated-connections", havingValue = "true")
public class RedisDedicatedConnectionConfig {

    @Bean
    static BeanPostProcessor lettuceShareNativeConnectionDisabler() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) {
                if (bean instanceof LettuceConnectionFactory factory) {
                    factory.setShareNativeConnection(false);
                }
                return bean;
            }
        };
    }
}
