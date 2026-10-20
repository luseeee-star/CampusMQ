package com.campusmq.broker;

import com.campusmq.broker.config.BrokerConfig;

/**
 * Broker 启动入口。
 * 用法：java BrokerStartup [-p port] [-s storePath] [-f SYNC|ASYNC_100MS|ASYNC_500MS]
 */
public final class BrokerStartup {

    private BrokerStartup() {
    }

    public static void main(String[] args) {
        BrokerConfig config = new BrokerConfig();
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "-p" -> config.setPort(Integer.parseInt(args[++i]));
                case "-s" -> config.setStorePath(args[++i]);
                case "-f" -> config.setFlushType(BrokerConfig.FlushType.valueOf(args[++i]));
                default -> {
                }
            }
        }
        BrokerController controller = new BrokerController(config);
        Runtime.getRuntime().addShutdownHook(new Thread(controller::shutdown, "shutdown-hook"));
        controller.start();
        System.out.println("CampusMQ broker is running. Ctrl+C to stop.");
    }
}
