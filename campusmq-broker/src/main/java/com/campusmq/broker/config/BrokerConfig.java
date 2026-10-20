package com.campusmq.broker.config;

/** Broker 配置。关键常量取值见执行方案 §2.2。 */
public class BrokerConfig {

    public enum FlushType {
        SYNC(0), ASYNC_100MS(100), ASYNC_500MS(500);

        private final long intervalMs;

        FlushType(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        public long intervalMs() {
            return intervalMs;
        }
    }

    private String host = "127.0.0.1";
    private int port = 10911;
    private String storePath = System.getProperty("user.home") + "/campusmq-store";

    /** CommitLog 单文件 128MB，便于快速观察文件滚动 */
    private int commitLogMappedFileSize = 128 * 1024 * 1024;
    /** ConsumeQueue 单文件：20B 定长条目 × 100000 条 = 2MB */
    private int consumeQueueMappedFileSize = 20 * 100000;

    private FlushType flushType = FlushType.SYNC;

    /** 消费进度持久化间隔（执行方案 §3.5：每 5 秒） */
    private long offsetPersistIntervalMs = 5_000L;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public int getCommitLogMappedFileSize() {
        return commitLogMappedFileSize;
    }

    public void setCommitLogMappedFileSize(int commitLogMappedFileSize) {
        this.commitLogMappedFileSize = commitLogMappedFileSize;
    }

    public int getConsumeQueueMappedFileSize() {
        return consumeQueueMappedFileSize;
    }

    public void setConsumeQueueMappedFileSize(int consumeQueueMappedFileSize) {
        this.consumeQueueMappedFileSize = consumeQueueMappedFileSize;
    }

    public FlushType getFlushType() {
        return flushType;
    }

    public void setFlushType(FlushType flushType) {
        this.flushType = flushType;
    }

    public long getOffsetPersistIntervalMs() {
        return offsetPersistIntervalMs;
    }
}
