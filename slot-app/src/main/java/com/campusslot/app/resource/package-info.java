/**
 * 资源与资格名单管理（管理端，需求书 §5.2）。
 *
 * <p>包级分层，不是独立 Maven 模块 —— 整个 API 侧只有 {@code slot-app} 一个模块，
 * 功能边界靠包而非工程来体现（单仓库 3 模块：common / app / consumer）。
 *
 * <p>落地日：D2。要做的四件事：
 * <ol>
 *   <li>G-1 创建活动：名称、开放时刻、总名额、确认时限、<b>按学院设配额</b>（{@code quota_college}）</li>
 *   <li>G-2 配置参与资格：导入学号名单，或按学籍规则筛选生成（{@code quota_roster}）</li>
 *   <li>G-3 查看发放进度：申请数 / 成功数 / 剩余量，数据来自 Redis 余量 + DB 计数</li>
 *   <li>G-4 导出对账报表：申请 / 成功 / 确认 / 释放 四项平衡（{@code quota_report}）</li>
 * </ol>
 *
 * <p>发布（DRAFT → PUBLISHED）时必须把余量灌进 Redis：
 * {@code quota:stock:{resourceId}} 与每条学院配额 {@code quota:college:{resourceId}:{collegeCode}}。
 * 少灌学院配额的表现是「所有学院的人都收到『系统繁忙』」——脚本返回 -3，而不是放行，
 * 这是刻意设计的：配置缺失时宁可拒绝，也不能悄悄超发。
 *
 * <p>硬约束：所有查询必须带 dept_id 维度，否则中台就退化成各部门各一套脚本。
 */
package com.campusslot.app.resource;
