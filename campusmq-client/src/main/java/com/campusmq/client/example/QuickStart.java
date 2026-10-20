package com.campusmq.client.example;

import com.campusmq.client.consumer.DefaultMQPullConsumer;
import com.campusmq.client.consumer.PullResult;
import com.campusmq.client.producer.DefaultMQProducer;
import com.campusmq.client.producer.SendResult;

/**
 * D1 验收示例：命令行收发消息。
 * 前置：BrokerStartup 已启动。
 * 用法：java com.campusmq.client.example.QuickStart [brokerAddr]
 */
public final class QuickStart {

    private QuickStart() {
    }

    public static void main(String[] args) throws Exception {
        String addr = args.length > 0 ? args[0] : "127.0.0.1:10911";
        String topic = "quickstart";

        DefaultMQProducer producer = new DefaultMQProducer(addr);
        producer.start();
        for (int i = 0; i < 5; i++) {
            SendResult r = producer.send(topic, "hello campusmq " + i);
            System.out.println("[send] " + r);
        }
        producer.shutdown();

        // reput 异步建索引，稍等
        Thread.sleep(300);

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("quickstart-group", addr);
        consumer.start();
        long offset = -1;
        int round = 0;
        while (round++ < 10) {
            PullResult r = consumer.pull(topic, 0, offset, 32);
            if (!r.found()) {
                break;
            }
            r.getMessages().forEach(m ->
                    System.out.println("[recv] queueOffset=" + m.getQueueOffset()
                            + " body=" + PullResult.bodyAsString(m)));
            offset = r.getNextOffset();
            consumer.ack(topic, 0, offset);
        }
        consumer.shutdown();
        System.out.println("QuickStart done.");
    }
}
