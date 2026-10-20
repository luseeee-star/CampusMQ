package com.campusmq.broker;

import com.campusmq.broker.config.BrokerConfig;
import com.campusmq.client.consumer.DefaultMQPullConsumer;
import com.campusmq.client.consumer.PullResult;
import com.campusmq.client.producer.DefaultMQProducer;
import com.campusmq.client.producer.SendResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * D1 + D5 验收：
 * 1. 命令行（测试即客户端）能收发消息；
 * 2. 两个消费组互不影响地消费同一主题；
 * 3. offset 持久化，Broker 重启后进度续费、消息不丢。
 */
class BrokerIntegrationTest {

    private static final String STORE_DIR = "target/test-store/broker-it";
    private static final String TOPIC = "quota_claimed";

    private int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private BrokerController startBroker(int port) {
        BrokerConfig config = new BrokerConfig();
        config.setPort(port);
        config.setStorePath(STORE_DIR);
        config.setFlushType(BrokerConfig.FlushType.SYNC);
        BrokerController controller = new BrokerController(config);
        controller.start();
        return controller;
    }

    @Test
    void sendPullAckAndRestartResume() throws Exception {
        com.campusmq.broker.store.TestStoreCleaner.clean(STORE_DIR);
        int port = freePort();
        String addr = "127.0.0.1:" + port;
        BrokerController broker = startBroker(port);

        // 1. 生产者发送 50 条
        DefaultMQProducer producer = new DefaultMQProducer(addr);
        producer.start();
        int total = 50;
        for (int i = 0; i < total; i++) {
            SendResult r = producer.send(TOPIC, "claim-" + i);
            assertTrue(r.isOk(), "send failed: " + r.getError());
            assertEquals(i, r.getQueueOffset());
        }

        // 2. 等异步建索引追上
        waitReput(broker, total);

        // 3. 消费组 notify 全量消费并 ACK
        DefaultMQPullConsumer notify = new DefaultMQPullConsumer("notify", addr);
        notify.start();
        PullResult r1 = notify.pull(TOPIC, 0, -1, 64);
        assertTrue(r1.found());
        assertEquals(total, r1.getMessages().size());
        assertEquals("claim-0", PullResult.bodyAsString(r1.getMessages().get(0)));
        assertTrue(notify.ack(TOPIC, 0, r1.getNextOffset()));

        // 4. 消费组 credit 独立消费（组间隔离），只确认前 20 条
        DefaultMQPullConsumer credit = new DefaultMQPullConsumer("credit", addr);
        credit.start();
        PullResult r2 = credit.pull(TOPIC, 0, -1, 64);
        assertEquals(total, r2.getMessages().size());
        assertTrue(credit.ack(TOPIC, 0, 20));

        // 5. notify 组再拉：没有新消息
        PullResult r3 = notify.pull(TOPIC, 0, -1, 64);
        assertFalse(r3.found());

        notify.shutdown();
        credit.shutdown();
        producer.shutdown();
        broker.shutdown();

        // 6. 重启 Broker：进度与消息都要在
        BrokerController broker2 = startBroker(port);
        waitReput(broker2, total);

        DefaultMQPullConsumer notify2 = new DefaultMQPullConsumer("notify", addr);
        notify2.start();
        PullResult r4 = notify2.pull(TOPIC, 0, -1, 64);
        assertFalse(r4.found(), "notify 组已消费完，重启后不应重复拉到");

        DefaultMQPullConsumer credit2 = new DefaultMQPullConsumer("credit", addr);
        credit2.start();
        PullResult r5 = credit2.pull(TOPIC, 0, -1, 64);
        assertTrue(r5.found());
        assertEquals(total - 20, r5.getMessages().size());
        assertEquals("claim-20", PullResult.bodyAsString(r5.getMessages().get(0)));

        // 7. 重启后 queueOffset 不回退
        DefaultMQProducer producer2 = new DefaultMQProducer(addr);
        producer2.start();
        SendResult r6 = producer2.send(TOPIC, "claim-after-restart");
        assertTrue(r6.isOk());
        assertEquals(total, r6.getQueueOffset());

        notify2.shutdown();
        credit2.shutdown();
        producer2.shutdown();
        broker2.shutdown();
    }

    private void waitReput(BrokerController broker, long expectQueueOffset) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (broker.getMessageStore().getMaxQueueOffset(TOPIC, 0) >= expectQueueOffset) {
                return;
            }
            Thread.sleep(50);
        }
        fail("reput not caught up in time");
    }
}
