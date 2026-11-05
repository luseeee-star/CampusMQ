package com.campusslot.app;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * API 侧唯一启动进程（端口 8080）。
 *
 * <p>整个工程只有两个可启动进程：本类和 {@code ConsumerApplication}。
 * 功能边界靠包体现，不靠工程：
 * <pre>
 *   app.gateway     身份解析与 traceId 落地
 *   app.resource    资源与资格名单管理（管理端）
 *   app.claim       申领与状态流转（核心链路）
 *   app.reconcile   对账巡检与报表
 * </pre>
 *
 * <p>为什么 API 侧全在一个进程里：业务服务无状态，状态全在 Redis / MySQL / MQ，
 * 所以「一个进程」与「五个进程」在水平扩展能力上没有区别，但本地调试、
 * 故障注入、部署复杂度差很多。真要按包独立扩时，把这些包整体挪进新模块即可，类不用改。
 *
 * <p>{@code @EnableScheduling} 承载需求书 §5.3 的两类系统任务：
 * SYS-3 对账巡检（每 5 分钟）与 SYS-1 超时释放扫表。
 * <b>注意：扫表是目前唯一的释放路径</b>——{@code quota_release_delay} 定时消息只在
 * 主题定义与生产者接口里存在，尚未接入发送点（计划中，见 D8 遗留）。
 * 定时任务在 2→8 台水平扩时会重复执行，必须靠「条件更新 + 受影响行数」幂等，
 * 不能靠「只有一个实例会跑」这种前提。
 */
@SpringBootApplication(scanBasePackages = "com.campusslot")
@MapperScan("com.campusslot.common.mapper")
@EnableScheduling
public class SlotApplication {

    public static void main(String[] args) {
        SpringApplication.run(SlotApplication.class, args);
    }
}
