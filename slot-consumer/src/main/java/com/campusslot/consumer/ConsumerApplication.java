package com.campusslot.consumer;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 消费侧独立进程。
 *
 * <p>独立进程而不是和 API 同进程的两个理由：
 * <ol>
 *   <li>D6 要验证「停掉通知消费者，申领仍成功」，同进程做不出真正的进程隔离</li>
 *   <li>D11 的 {@code kill -STOP} 消费者假死实验，只有独立进程才做得了</li>
 * </ol>
 */
@SpringBootApplication(scanBasePackages = "com.campusslot")
@MapperScan("com.campusslot.common.mapper")
@EnableScheduling
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
