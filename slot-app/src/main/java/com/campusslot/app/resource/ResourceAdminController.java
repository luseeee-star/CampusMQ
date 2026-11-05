package com.campusslot.app.resource;

import com.campusslot.app.resource.dto.CreateResourceRequest;
import com.campusslot.app.resource.dto.CreateResourceResult;
import com.campusslot.app.resource.dto.PublishResult;
import com.campusslot.common.api.R;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端两个接口：建活动（G-1 + G-2）、发布（DRAFT → PUBLISHED）。
 *
 * <p>导对账（G-4）不在这里，它在 D9 的报表口径上另开一个薄壳——两者的输入完全不同：
 * 这里写配置，那边读统计，混在一个 Controller 里会让"发布失败"和"报表打不开"
 * 看起来像同一类问题。
 *
 * <p><b>没有鉴权，这是明确的取舍，不是忘了</b>：现在系统里没有管理端身份体系
 * （{@code StudentContext} 只有学号，见网关过滤器），写一个"只能建本部门的活动"的检查
 * 只是装饰——请求方改个 deptId 就绕过去了，伪造成本为零。
 * 所以注释和报告里都写"鉴权未做，只校验部门存在"，把边界摆出来，比假装做了强。
 * 上线前这一条必须先补，否则任何人都能建活动、发布、把库存 key 灌成任意值。
 */
@RestController
@RequestMapping("/api/admin/resources")
public class ResourceAdminController {

    private final ResourceAdminService service;

    public ResourceAdminController(ResourceAdminService service) {
        this.service = service;
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
}
