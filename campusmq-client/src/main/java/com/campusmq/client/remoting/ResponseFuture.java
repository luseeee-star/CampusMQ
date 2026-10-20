package com.campusmq.client.remoting;

import com.campusmq.common.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步转同步（执行方案 §4.1）：Netty 的收发在不同线程，
 * 用 opaque -> ResponseFuture 的映射把响应物归原位。
 */
public class ResponseFuture {

    private final int opaque;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RemotingCommand response;

    public ResponseFuture(int opaque) {
        this.opaque = opaque;
    }

    public RemotingCommand await(long timeoutMs) throws InterruptedException {
        boolean done = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        return done ? response : null;
    }

    public void complete(RemotingCommand response) {
        this.response = response;
        latch.countDown();
    }

    public int getOpaque() {
        return opaque;
    }
}
