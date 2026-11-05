package com.campusslot.app.gateway.filter;

import com.campusslot.app.gateway.ratelimit.RateLimitProperties;
import com.campusslot.app.gateway.ratelimit.TokenBucketRateLimiter;
import com.campusslot.common.api.ErrorCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * D6 入口限流。
 *
 * <p><b>为什么排在最前面（{@code @Order(0)}，先于身份解析）。</b>
 * 限流要回答的问题只有一个：这个请求现在能不能进来。它不需要学生是谁、
 * 也不该等 Spring MVC 把参数解析完。放在 Tomcat 线程池之后、DispatcherServlet 之前，
 * 被拒请求的开销就只剩「一次 URI 匹配 + 一次 CAS + 写 4 字节」，
 * 这是「拒绝 RT &lt; 5ms」能成立的唯一做法 —— 只要请求进了 Controller，
 * 光 Redis 往返就 0.3ms 起，再加上排队，5ms 根本守不住。
 *
 * <p><b>为什么拒绝用 429 而不是 200 + 业务码。</b>429 是 HTTP 语义里专门为"你发太快了"
 * 留的状态码，网关、CDN、客户端重试库都认它；塞进 200 里会让所有中间层把这次拒绝
 * 当成正常响应缓存下来。但 body 仍然保持 {@link com.campusslot.common.api.R} 的结构，
 * 前端不用为限流单独写一套解析。
 *
 * <p><b>R-8（明确提示）落在这里。</b>返回码用 {@link ErrorCode#RATE_LIMITED}(3001)
 * 「当前人数过多，请稍后再试」，<b>绝不能复用 2001「名额已满」</b> ——
 * 那会让学生以为东西抢光了，实际只是系统在限速，而且名额可能还有。
 * 3xxx 段在设计时就约定「主动拒绝、不计入 NFR-2 失败率」，见 {@link ErrorCode#isActiveRejection()}。
 */
@Component
@Order(0)
public class RateLimitFilter extends OncePerRequestFilter {

    private static final String RETRY_AFTER_SECONDS = "1";

    private final RateLimitProperties props;
    private final TokenBucketRateLimiter limiter;
    private final AntPathMatcher matcher = new AntPathMatcher();

    private final Counter allowedCounter;
    private final Counter rejectedCounter;
    /**
     * 判定本身耗时（不含网络、不含响应写出）。
     *
     * <p>为什么要有这个指标：端到端测出来的「拒绝 RT」必然包含压测线程调度、连接复用、
     * 内核 accept 队列这些和限流器无关的东西。第一轮实测 P99=10ms，但 P50=1ms ——
     * 只看端到端会得出"限流器慢"的错误结论。有了这个 Timer 才能拆开：
     * 判定只要几百纳秒，那 10ms 是压测器自己的抖动。
     */
    private final Timer decisionTimer;

    public RateLimitFilter(RateLimitProperties props, MeterRegistry meterRegistry) {
        this.props = props;
        this.limiter = new TokenBucketRateLimiter(props.getQps(), props.getBurst());
        this.allowedCounter = Counter.builder("campusslot_gateway_rate_limit_total")
                .description("D6 入口限流判定次数")
                .tag("result", "allowed")
                .register(meterRegistry);
        this.rejectedCounter = Counter.builder("campusslot_gateway_rate_limit_total")
                .description("D6 入口限流判定次数")
                .tag("result", "rejected")
                .register(meterRegistry);
        this.decisionTimer = Timer.builder("campusslot_gateway_rate_limit_decision")
                .description("D6 限流判定耗时（服务端，不含网络）")
                .register(meterRegistry);
    }

    /**
     * 命中成本必须极低：每个请求都会走一次。
     * 顺序是先判开关、再判排除、最后判包含 —— 关掉限流时（默认）只多一次 boolean 判断。
     */
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (!props.isEnabled()) {
            return true;
        }
        String uri = request.getRequestURI();
        for (String pattern : props.getExcludePaths()) {
            if (matcher.match(pattern, uri)) {
                return true;
            }
        }
        for (String pattern : props.getIncludePaths()) {
            if (matcher.match(pattern, uri)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        long t0 = System.nanoTime();
        boolean allowed = limiter.tryAcquire();
        decisionTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
        if (allowed) {
            allowedCounter.increment();
            chain.doFilter(request, response);
            return;
        }
        rejectedCounter.increment();
        writeRejection(request, response);
    }

    /**
     * 手写 JSON 而不是走 ObjectMapper。
     *
     * <p>不是迷信性能：{@code ObjectMapper} 首次调用要建序列化器，之后每次也有反射/缓存查找，
     * 在"拒绝路径必须是全链路最短"的前提下，一个固定形状的 4 字段响应没有理由引入它。
     * 代价是字段名硬编码 —— 所以这里只放 code / message / traceId 三个稳定字段，
     * 业务数据一个都不放。
     */
    private void writeRejection(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String traceId = request.getHeader(StudentContextFilter.TRACE_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }
        response.setStatus(429);
        response.setHeader(StudentContextFilter.TRACE_ID_HEADER, traceId);
        response.setHeader("Retry-After", RETRY_AFTER_SECONDS);
        // 让客户端知道上限是多少，便于压测脚本核对"放行精度"，也便于前端做退避
        response.setHeader("X-Rate-Limit-Limit", String.valueOf(props.getQps()));
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write("{\"code\":" + ErrorCode.RATE_LIMITED.getCode()
                + ",\"message\":\"" + ErrorCode.RATE_LIMITED.getMessage()
                + "\",\"traceId\":\"" + traceId + "\"}");
    }

    /** 暴露给 dev 端点做实时观测：剩余令牌与累计放行/拒绝数。 */
    public TokenBucketRateLimiter limiter() {
        return limiter;
    }
}
