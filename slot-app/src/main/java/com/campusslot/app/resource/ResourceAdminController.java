package com.campusslot.app.resource;

import com.campusslot.app.resource.dto.CreateResourceRequest;
import com.campusslot.app.resource.dto.CreateResourceResult;
import com.campusslot.app.resource.dto.ProgressView;
import com.campusslot.app.resource.dto.PublishResult;
import com.campusslot.common.api.R;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

/**
 * 管理端四个接口：建活动（G-1 + G-2）、发布、发放进度（G-3）、对账导出（G-4）。
 *
 * <p>后两个是只读的，放在这里是因为它们<b>用的是同一套活动口径</b>：同一个
 * {@code resourceId}、同一个 {@code deptId}、同一份配置。拆去 D9 的报表控制器会让
 * "这个活动"在两条路径上各有一份取数逻辑，两边的数对不上时没人说得清谁对。
 * 真正的差别是：这里按活动维度算，D9 的巡检按全库维度算——两者不是同一件事。
 *
 * <p><b>没有鉴权，这是明确的取舍，不是忘了</b>：现在系统里没有管理端身份体系
 * （{@code StudentContext} 只有学号，见网关过滤器），写一个"只能建本部门的活动"的检查
 * 只是装饰——请求方改个 deptId 就绕过去了，伪造成本为零。
 * 所以注释和报告里都写"鉴权未做，只校验部门存在"，把边界摆出来，比假装做了强。
 * 上线前这一条必须先补，否则任何人都能建活动、发布、把库存 key 灌成任意值，
 * 也能把全校的申领名单导走。
 */
@RestController
@RequestMapping("/api/admin/resources")
public class ResourceAdminController {

    private final ResourceAdminService service;
    private final ResourceReportService report;

    public ResourceAdminController(ResourceAdminService service, ResourceReportService report) {
        this.service = service;
        this.report = report;
    }

    /** 建一个草稿活动；名单与学院配额随请求体一起进来。 */
    @PostMapping
    public R<CreateResourceResult> create(@Valid @RequestBody CreateResourceRequest request) {
        return R.ok(service.create(request));
    }

    /**
     * 发布：灌三类 key（余量 / 学院 / 名单）→ 迁移状态 → 清元信息缓存。
     *
     * <p>用 POST 而不是 PUT：它不是"把资源替换成请求体里的样子"，而是触发一次动作，
     * 而且对 DRAFT 和 PUBLISHED 的语义不同（覆盖 / 只补缺）。用 PUT 会让人以为可以重复提交任意内容。
     */
    @PostMapping("/{resourceId}/publish")
    public R<PublishResult> publish(@PathVariable long resourceId) {
        return R.ok(service.publish(resourceId));
    }

    /**
     * 发放进度：三套账并列（DB 余量、Redis 余量、由记录反推的应有余额）。
     *
     * @param deptId 可选。传了就必须和活动所属部门一致，不一致报参数错误而不是返回 0——
     *               返回 0 会被读成"这个部门真没人领"
     */
    @GetMapping("/{resourceId}/progress")
    public R<ProgressView> progress(@PathVariable long resourceId,
                                    @RequestParam(required = false) Long deptId) {
        return R.ok(report.progress(resourceId, deptId));
    }

    /**
     * 对账导出（CSV）。
     *
     * <p>走 HTTP 下载而不是写到服务器磁盘：写文件要处理"写到哪个目录、谁能读、什么时候清"，
     * 而现在这个系统连管理端身份都没有，落盘等于把全校名单留在机器上。
     *
     * @param groupBy {@code college}（默认）按学院分行；{@code none} 只出合计
     */
    @GetMapping("/{resourceId}/export.csv")
    public ResponseEntity<String> export(@PathVariable long resourceId,
                                         @RequestParam(required = false) Long deptId,
                                         @RequestParam(required = false, defaultValue = "college") String groupBy) {
        String csv = report.exportCsv(resourceId, deptId, groupBy);
        String filename = "resource-" + resourceId + "-reconcile.csv";
        // 中文列名 + Excel：BOM 在 exportCsv 里已经加了，这里只声明 charset。
        // 文件名保持 ASCII——Content-Disposition 里的中文要么 RFC 5987 编码，
        // 要么就变成乱码，而这台机器上没有统一的编码约定
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(new MediaType("text", "csv", StandardCharsets.UTF_8))
                .body(csv);
    }
}
