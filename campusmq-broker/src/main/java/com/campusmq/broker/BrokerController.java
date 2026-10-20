package com.campusmq.broker;

import com.campusmq.broker.config.BrokerConfig;
import com.campusmq.broker.consume.ConsumerOffsetManager;
import com.campusmq.broker.netty.BrokerNettyServer;
import com.campusmq.broker.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Broker 总装：配置 -> 存储 -> 消费进度 -> 网络层。 */
public class BrokerController {

    private static final Logger log = LoggerFactory.getLogger(BrokerController.class);

    private final BrokerConfig config;
    private final DefaultMessageStore messageStore;
    private final ConsumerOffsetManager offsetManager;
    private final BrokerNettyServer nettyServer;

    public BrokerController(BrokerConfig config) {
        this.config = config;
        this.messageStore = new DefaultMessageStore(config);
        this.offsetManager = new ConsumerOffsetManager(config);
        this.nettyServer = new BrokerNettyServer(this);
    }

    public void start() {
        messageStore.load();
        messageStore.start();
        offsetManager.load();
        offsetManager.start();
        nettyServer.start();
        log.info("broker controller started, port={}, storePath={}, flush={}",
                config.getPort(), config.getStorePath(), config.getFlushType());
    }

    public void shutdown() {
        nettyServer.shutdown();
        offsetManager.shutdown();
        messageStore.shutdown();
        log.info("broker controller shutdown");
    }

    public BrokerConfig getConfig() {
        return config;
    }

    public DefaultMessageStore getMessageStore() {
        return messageStore;
    }

    public ConsumerOffsetManager getOffsetManager() {
        return offsetManager;
    }
}
