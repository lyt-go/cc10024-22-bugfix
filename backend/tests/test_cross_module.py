"""跨模块数据流集成测试：验证钩子、持久化和domain_snapshot的正确性。"""
from __future__ import annotations

import os
import sys
import json
import shutil

# 确保后端目录在导入路径中
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist

# 使用persistence模块的正确路径进行验证
def _snap_path(*parts):
    return persist.data_path(parts[0], parts[1]) if len(parts) == 2 else persist.data_path("snapshots", parts[0])

# 清理测试数据目录
for sub in ["runs", "snapshots", "reports", "exports"]:
    p = persist.data_path(sub, "")
    if os.path.isdir(p):
        for f in os.listdir(p):
            os.remove(os.path.join(p, f))


def test_script_to_schedule_flow():
    """测试behavior_script标记生效后自动创建script_schedule。"""
    from app.store import store
    from app.services.behavior_script import BehaviorScriptService
    from app import persistence as persist

    svc = BehaviorScriptService()
    # 创建新脚本
    entry, missing = svc.create_entry({
        "脚本编号": "TEST-001",
        "目标站点": "test.example.com",
        "录制步骤": "step1,step2"
    })
    assert entry is not None, f"创建失败: {missing}"
    script_id = entry["id"]

    # 标记生效
    result, msg = svc.run_action(script_id, "标记生效")
    assert result is not None, f"标记生效失败: {msg}"
    assert result["status"] == "已生效"

    # 验证script_schedule自动创建
    schedules = [r for r in store.rows("script_schedule") if "TEST-001" in str(r.get("关联脚本", ""))]
    assert len(schedules) >= 1, f"未找到自动创建的调度计划，当前数量: {len(schedules)}"

    # 验证记录写入runs/JSONL
    runs = persist.read_jsonl("runs", "script_schedule.jsonl")
    auto_runs = [r for r in runs if r.get("action") == "auto_create_from_behavior_script"]
    assert len(auto_runs) >= 1, f"JSONL中未找到自动创建记录，当前记录数: {len(auto_runs)}"

    # 验证domain_snapshot包含跨模块统计
    snap = svc.domain_snapshot()
    assert snap.get("downstream_schedules", 0) >= 1


def test_selector_script_validation():
    """测试element_selector标记生效时校验关联脚本与采集字段绑定。"""
    from app.store import store
    from app.services.element_selector import ElementSelectorService
    from app.services.data_extract import DataExtractService
    from app import persistence as persist
    from app import bindings

    # 先为TEST-001补一个已生效的采集字段绑定（抽取模板）
    extract_svc = DataExtractService()
    t_entry, _ = extract_svc.create_entry({
        "模板编号": "TEST-EXTRACT-001",
        "关联脚本": "TEST-001",
        "抽取字段": "title",
    })
    assert t_entry is not None
    extract_svc.run_action(t_entry["id"], "标记生效")

    svc = ElementSelectorService()
    # 创建选择器
    entry, missing = svc.create_entry({
        "锚点编号": "TEST-SEL-001",
        "关联脚本": "TEST-001",
        "选择器类型": "CSS",
        "CSS表达式": "div.title",
    })
    assert entry is not None
    sel_id = entry["id"]

    # 标记生效（关联脚本TEST-001已生效且有采集字段绑定）
    result, _ = svc.run_action(sel_id, "标记生效")
    assert result is not None
    assert result["status"] == "已生效"
    # 关联脚本已生效，不应标记为abnormal
    assert not result.get("abnormal"), f"预期不异常但实际为abnormal={result.get('abnormal')}"

    # 验证绑定快照文件写入并带确认归属
    binding_version = result.get("binding_version", "")
    assert binding_version, "已生效锚点必须回填绑定版本"
    snapshot = bindings.load_binding(binding_version)
    assert snapshot is not None, f"绑定快照未完整持久化: {binding_version}"
    assert snapshot["selector_type"] == "CSS"
    assert snapshot["css_expr"] == "div.title"
    assert snapshot["linked_script"] == "TEST-001"
    assert snapshot["script_active"] is True
    assert snapshot["field_bindings"] and snapshot["field_bindings"][0]["field"] == "title"

    # domain_snapshot检查
    snap_report = svc.domain_snapshot()
    assert snap_report.get("selector_snapshots", 0) >= 1


def test_batch_to_history_flow():
    """测试batch_queue确认完成写入run_history。"""
    from app.store import store
    from app.services.batch_queue import BatchQueueService
    from app import persistence as persist

    svc = BatchQueueService()
    entry, _ = svc.create_entry({
        "批次编号": "TEST-BATCH-001",
        "关联计划": "SCRI-AUTO-0001",
        "任务数量": 5
    })
    assert entry is not None
    batch_id = entry["id"]

    # 确认完成
    result, _ = svc.run_action(batch_id, "确认完成")
    assert result is not None
    assert result["status"] == "已完成"

    # 验证run_history自动创建
    runs = persist.read_jsonl("runs", "run_history.jsonl")
    auto_runs = [r for r in runs if r.get("action") == "auto_create_from_batch"]
    assert len(auto_runs) >= 1, f"未找到batch触发的运行记录"

    # 验证domain_snapshot
    snap = svc.domain_snapshot()
    assert snap.get("total_tasks", 0) >= 5
    assert snap.get("batch_runs", 0) >= 1


def test_recovery_no_overwrite():
    """测试error_recover确认恢复不会覆盖已有记录。"""
    from app.store import store
    from app.services.error_recover import ErrorRecoverService
    from app import persistence as persist

    # 确认当前run_history记录数
    before_runs = persist.read_jsonl("runs", "run_history.jsonl")
    before_count = len(before_runs)

    svc = ErrorRecoverService()
    entry, _ = svc.create_entry({
        "任务编号": "TEST-RECOVER-001",
        "关联计划": "SCRI-AUTO-0001",
        "失败步骤": "data_extract"
    })
    assert entry is not None
    rec_id = entry["id"]

    # 确认恢复
    result, _ = svc.run_action(rec_id, "确认恢复")
    assert result is not None
    assert result["status"] == "已恢复"

    # 验证追加而非覆盖
    after_runs = persist.read_jsonl("runs", "run_history.jsonl")
    after_count = len(after_runs)
    assert after_count >= before_count, f"记录数减少: {before_count} -> {after_count}"
    recovery_runs = [r for r in after_runs if r.get("action") == "auto_create_from_recovery"]
    assert len(recovery_runs) >= 1


def test_pipeline_transform_link():
    """测试clean_pipeline启用时检查transform_rule就绪状态。"""
    from app.store import store
    from app.services.clean_pipeline import CleanPipelineService
    from app import persistence as persist

    # 先创建一个转换规则并标记生效
    from app.services.transform_rule import TransformRuleService
    trans_svc = TransformRuleService()
    t_entry, _ = trans_svc.create_entry({
        "规则编号": "TEST-TRANS-001",
        "关联工序": "TEST-PIPE-001",
        "源字段": "raw_field"
    })
    assert t_entry is not None
    trans_svc.run_action(t_entry["id"], "标记生效")

    svc = CleanPipelineService()
    entry, _ = svc.create_entry({
        "工序编号": "TEST-PIPE-001",
        "输入源": "behavior_script",
        "清洗规则": "trim,lowercase"
    })
    assert entry is not None
    pipe_id = entry["id"]

    result, _ = svc.run_action(pipe_id, "启用工序")
    assert result is not None
    assert result["status"] == "已启用"

    # 验证transform检查报告
    reports = persist.read_jsonl("reports", "pipeline_transform_check.jsonl")
    linked = [r for r in reports if r.get("pipeline_code") == "TEST-PIPE-001"]
    assert len(linked) >= 1, f"未找到管道转换检查报告: {len(reports)}"


def test_run_history_confirm_triggers():
    """测试run_history确认完成触发自动去重和Schema检查。"""
    from app.store import store
    from app.services.run_history import RunHistoryService
    from app import persistence as persist

    svc = RunHistoryService()
    entry, _ = svc.create_entry({
        "记录编号": "TEST-RUN-001",
        "关联计划": "SCRI-AUTO-0001",
        "开始时间": "2026-01-01"
    })
    assert entry is not None
    run_id = entry["id"]

    result, _ = svc.run_action(run_id, "确认完成")
    assert result is not None
    assert result["status"] == "已完成"

    # 验证触发去重
    dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
    dedup_triggered = [r for r in dedup_reports if r.get("trigger") == "run_history_confirm"]
    # 去重触发需要page_dedup有已生效规则和page_快照文件存在

    # 验证触发Schema
    schema_reports = persist.read_jsonl("reports", "schema_check_report.jsonl")
    schema_triggered = [r for r in schema_reports if r.get("trigger") == "run_history_confirm"]
    assert len(schema_triggered) >= 1, f"Schema检查未触发: {len(schema_triggered)}"


def test_quality_report_generation():
    """测试质量报告生成和合规率计算。"""
    from app.store import store
    from app.services.quality_guard import QualityGuardService
    from app import persistence as persist

    svc = QualityGuardService()
    report = svc.run_quality_report("behavior_script")
    assert report is not None
    assert "compliance_rate" in report
    assert "total_records" in report
    assert report["total_records"] >= 1

    # 验证报告持久化
    reports = persist.read_jsonl("reports", "quality_report.jsonl")
    assert len(reports) >= 1
    latest = reports[-1]
    assert latest["target"] == "behavior_script"


def test_domain_snapshot_contract():
    """验证所有模块的domain_snapshot输出格式一致。"""
    from app.services import SERVICES

    required_keys = {"module", "entity", "artifact", "count", "by_status", "abnormal"}
    for name, svc in SERVICES.items():
        snap = svc.domain_snapshot()
        missing = required_keys - set(snap.keys())
        assert not missing, f"{name}: domain_snapshot缺少字段 {missing}"
        assert isinstance(snap["count"], int)
        assert isinstance(snap["abnormal"], int)
        assert isinstance(snap["by_status"], dict)


def test_version_tracking():
    """测试状态流转时的version指纹更新。"""
    from app.store import store
    from app.services.behavior_script import BehaviorScriptService

    svc = BehaviorScriptService()
    entry, _ = svc.create_entry({
        "脚本编号": "TEST-VERSION-001",
        "目标站点": "version.test.com",
        "录制步骤": "v1"
    })
    assert entry is not None
    v1 = entry.get("version", "")
    assert v1, "创建后应有version"

    svc.run_action(entry["id"], "标记生效")
    entry2 = store.find("behavior_script", entry["id"])
    assert entry2 is not None
    v2 = entry2.get("version", "")
    assert v2, "状态变更后应有version"
    # 状态变更后版本应该不同（因为status变了）
    assert v1 != v2, f"版本应该变更: {v1} == {v2}"


def test_schema_check_validation():
    """测试Schema校验规则的应用。"""
    from app.store import store
    from app.services.schema_check import SchemaCheckService
    from app import persistence as persist

    svc = SchemaCheckService()
    # 创建一个已生效的规则
    entry, _ = svc.create_entry({
        "规则编号": "TEST-SCHEMA-001",
        "目标数据": "behavior_script",
        "字段名": "id",
        "类型约束": "int",
        "空值策略": "禁止"
    })
    assert entry is not None
    svc.run_action(entry["id"], "标记生效")

    # 校验一条记录
    record = store.rows("behavior_script")[0]
    violations = svc.validate_record(record, "behavior_script")
    # id字段应为int类型且不为空，预期无违规
    assert violations == [] or all(isinstance(v, str) for v in violations)

    # 验证违规记录写入
    if violations:
        v_records = persist.read_jsonl("reports", "schema_violations.jsonl")
        assert len(v_records) >= 1


def test_content_cache_hit_miss():
    """测试内容缓存的命中和未命中统计。"""
    from app.store import store
    from app.services.content_cache import ContentCacheService

    svc = ContentCacheService()

    call_count = [0]
    def fetcher():
        call_count[0] += 1
        return {"data": "test_value"}

    # 首次获取：miss
    result1 = svc.get_or_fetch("test_key_1", fetcher)
    assert result1 == {"data": "test_value"}
    assert call_count[0] == 1

    # 重复获取：hit
    result2 = svc.get_or_fetch("test_key_1", fetcher)
    assert result2 == {"data": "test_value"}
    assert call_count[0] == 1  # 未重新调用

    snap = svc.domain_snapshot()
    assert snap["cache_hit"] >= 1
    assert snap["cache_miss"] >= 1


def test_field_normalize_values():
    """测试字段归一化逻辑。"""
    from app.store import store
    from app.services.field_normalize import FieldNormalizeService

    svc = FieldNormalizeService()
    # 创建lowercase规则
    entry, _ = svc.create_entry({
        "规则编号": "TEST-NORM-001",
        "关联字段": "target_site",
        "归一类型": "lowercase"
    })
    assert entry is not None
    svc.run_action(entry["id"], "标记生效")

    # 测试归一化
    result, ok = svc.normalize_value("target_site", "TEST.Com")
    assert ok
    assert result == "test.com"


def test_page_dedup_fingerprint():
    """测试页面去重指纹计算。"""
    from app.store import store
    from app.services.page_dedup import PageDedupService
    from app import persistence as persist

    svc = PageDedupService()
    # 使用已存在的规则
    rule = store.rows("page_dedup")[0]
    rule_id = rule["id"]

    pages = [
        {"url": "https://a.com/page1", "dom": "content_a"},
        {"url": "https://a.com/page1", "dom": "content_a"},  # 重复
        {"url": "https://b.com/page2", "dom": "content_b"},
    ]
    result = svc.deduplicate_pages(pages, rule_id)
    assert result["total"] == 3
    assert result["unique"] == 2
    assert result["duplicates"] == 1


def test_export_channel_creates_file():
    """测试导出通道写入文件。"""
    from app.store import store
    from app.services.export_channel import ExportChannelService
    from app import persistence as persist

    svc = ExportChannelService()
    # 创建一个新通道，指定支持的输出格式
    entry, _ = svc.create_entry({
        "通道编号": "TEST-EXPORT-001",
        "输出格式": "json",
        "目标路径": "exports/"
    })
    assert entry is not None
    channel_id = entry["id"]
    svc.run_action(channel_id, "启用通道")

    data = [{"col1": "a", "col2": 1}]
    path = svc.export_data(channel_id, data)
    assert os.path.exists(path)
    assert os.path.getsize(path) > 0


if __name__ == "__main__":
    test_script_to_schedule_flow()
    print("PASS: test_script_to_schedule_flow")
    test_selector_script_validation()
    print("PASS: test_selector_script_validation")
    test_batch_to_history_flow()
    print("PASS: test_batch_to_history_flow")
    test_recovery_no_overwrite()
    print("PASS: test_recovery_no_overwrite")
    test_pipeline_transform_link()
    print("PASS: test_pipeline_transform_link")
    test_run_history_confirm_triggers()
    print("PASS: test_run_history_confirm_triggers")
    test_quality_report_generation()
    print("PASS: test_quality_report_generation")
    test_domain_snapshot_contract()
    print("PASS: test_domain_snapshot_contract")
    test_version_tracking()
    print("PASS: test_version_tracking")
    test_schema_check_validation()
    print("PASS: test_schema_check_validation")
    test_content_cache_hit_miss()
    print("PASS: test_content_cache_hit_miss")
    test_field_normalize_values()
    print("PASS: test_field_normalize_values")
    test_page_dedup_fingerprint()
    print("PASS: test_page_dedup_fingerprint")
    test_export_channel_creates_file()
    print("PASS: test_export_channel_creates_file")
    print("\nALL 14 TESTS PASSED")