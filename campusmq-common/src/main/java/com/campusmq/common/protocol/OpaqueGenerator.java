package com.campusmq.common.protocol;

import java.util.concurrent.atomic.AtomicInteger;

/** 请求序号生成器，服务端用 Map&lt;opaque, ResponseFuture&gt; 把响应物归原位。 */
public final class OpaqueGenerator {

    private static final AtomicInteger SEQ = new AtomicInteger(0);

    private OpaqueGenerator() {
    }

    public static int next() {
        return SEQ.incrementAndGet();
    }
}
