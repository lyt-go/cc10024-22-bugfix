"""锚点绑定版本与调度编排的端到端场景测试。

复现任务场景：
1. 先激活指向不存在脚本的锚点 → 必须可查地失败，不能显示成已生效。
2. 补录脚本后交叉发起脚本激活与锚点重试，并阻断一次快照写入 → 失败可核对、计划不可启动。
3. 恢复后启动运行 → 直接核对批次收到的表达式与快照版本。
4. 更换表达式 → 旧计划仍能回查自己的原绑定；同一锚点重试不产生两份可执行定位。
"""
from __future__ import annotations

import os
import sys
import threading

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import bindings
from app import persistence as persist

# 每个测试进程独立清理绑定产物目录，保证断言干净。
_bind_dir = persist.data_path(bindings.BINDING_SUB, "")
if os.path.isdir(_bind_dir):
    for f in os.listdir(_bind_dir):
        os.remove(os.path.join(_bind_dir, f))

from app.services.behavior_script import BehaviorScriptService
from app.services.element_selector import ElementSelectorService
from app.services.data_extract import DataExtractService
from app.services.script_schedule import ScriptScheduleService
from app.store import store

SUFFIX = os.getpid()


def _code(prefix: str, tag: str) -> str:
    return f"{prefix}-E2E-{SUFFIX}-{tag}"


def _find_schedule(script_code: str):
    rows = [r for r in store.rows("script_schedule") if r.get("关联脚本") == script_code]
    assert rows, f"脚本 {script_code} 未生成调度计划"
    return rows[-1]


def test_anchor_on_missing_script_is_rejected_and_auditable():
    """场景1：关联脚本不存在时，标记生效必须失败且可核对，不显示成已生效。"""
    sel_svc = ElementSelectorService()
    ghost = _code("GHOST", "1")
    entry, _ = sel_svc.create_entry({
        "锚点编号": _code("ELEM", "ghost"),
        "关联脚本": ghost,
        "选择器类型": "CSS",
        "CSS表达式": ".ghost",
    })
    assert entry is not None
    result, message = sel_svc.run_action(entry["id"], "标记生效")

    # 动作被阻断：没有返回生效后的条目，错误信息可查
    assert result is None, "不存在的脚本不应让锚点生效"
    assert ghost in message and "不存在" in message

    row = store.find("element_selector", entry["id"])
    assert row is not None
    assert row["status"] != "已生效", "工作台不能把选择器错误显示成已生效"
    assert row.get("abnormal") is True

    # 失败事件留在可核对范围，且没有产生任何绑定快照文件
    failed = [r for r in bindings.unconfirmed_records()
              if r.get("anchor_id") == entry["id"] and r.get("event") == "validate_failed"]
    assert failed, "失败绑定未登记，无法核对"
    assert not any(f.startswith("binding_") for f in persist.list_files(bindings.BINDING_SUB, ".json"))
    print("PASS: 场景1 不存在脚本的锚点确认被阻断并可核对")


def test_cross_activation_retry_with_interrupted_persistence():
    """场景2/3：补录脚本后交叉激活+重试，阻断一次快照写入，再恢复并启动核对批次。"""
    script_code = _code("BEHA", "main")
    anchor_code = _code("ELEM", "main")
    extract_code = _code("DATA", "main")

    script_svc = BehaviorScriptService()
    sel_svc = ElementSelectorService()
    extract_svc = DataExtractService()
    sched_svc = ScriptScheduleService()

    # 操作员先提交锚点（脚本尚未补录）
    anchor, _ = sel_svc.create_entry({
        "锚点编号": anchor_code,
        "关联脚本": script_code,
        "选择器类型": "CSS",
        "CSS表达式": "div.content > span.name",
        "XPath表达式": "//div[@class='content']/span",
    })
    assert anchor is not None

    # 另一人补录脚本与采集字段绑定
    script, _ = script_svc.create_entry({
        "脚本编号": script_code,
        "目标站点": "e2e.example.com",
        "录制步骤": "open,click",
    })
    assert script is not None
    template, _ = extract_svc.create_entry({
        "模板编号": extract_code,
        "关联脚本": script_code,
        "抽取字段": "name",
        "提取方式": "css",
    })
    assert template is not None
    extract_svc.run_action(template["id"], "标记生效")

    # 交叉发起：线程A激活脚本，线程B在脚本生效后立刻重试锚点确认（阻断一次快照写入）。
    # 两个动作仍在同一时间窗交叉编排，仅用事件保证阻断点稳定落在「快照落库」阶段。
    bindings.arm_write_failure()
    barrier = threading.Barrier(2)
    script_active = threading.Event()

    def activate_script():
        barrier.wait()
        script_svc.run_action(script["id"], "标记生效")
        script_active.set()

    def retry_anchor():
        barrier.wait()
        script_active.wait(timeout=5)
        sel_svc.run_action(anchor["id"], "标记生效")

    t1 = threading.Thread(target=activate_script)
    t2 = threading.Thread(target=retry_anchor)
    t1.start(); t2.start(); t1.join(); t2.join()

    schedule = _find_schedule(script_code)
    anchor_row = store.find("element_selector", anchor["id"])

    # 快照落库中断：锚点未生效，失败可核对；自动计划处于未绑定且不可启动
    persist_failed = [r for r in bindings.unconfirmed_records()
                      if r.get("anchor_id") == anchor["id"] and r.get("event") == "persist_failed"]
    assert persist_failed, "快照写入中断未留下可核对记录"
    assert anchor_row["status"] != "已生效"
    assert anchor_row.get("abnormal") is True
    assert schedule.get("binding_status") == "未绑定"

    denied, deny_msg = sched_svc.run_action(schedule["id"], "启动运行")
    assert denied is None and "完整采集字段绑定" in deny_msg
    assert any(r.get("event") == "startup_denied" and r.get("plan_code") == schedule["计划编号"]
               for r in bindings.unconfirmed_records())

    # 恢复：重新确认锚点 → 绑定完整持久化，锚点才生效
    ok_entry, ok_msg = sel_svc.run_action(anchor["id"], "标记生效")
    assert ok_entry is not None, f"恢复后确认应成功: {ok_msg}"
    binding_version = ok_entry["binding_version"]
    assert bindings.load_binding(binding_version) is not None

    # 同一锚点并发重试确认：内容指纹幂等，只产生一份可执行定位
    seen_versions: list[str] = []
    versions_lock = threading.Lock()

    def concurrent_retry():
        out, _ = sel_svc.run_action(anchor["id"], "标记生效")
        with versions_lock:
            seen_versions.append(out["binding_version"] if out else "")

    threads = [threading.Thread(target=concurrent_retry) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(seen_versions) == {binding_version}, f"并发重试产生了多份定位: {seen_versions}"
    confirmed_events = [r for r in persist.read_jsonl(bindings.BINDING_SUB, bindings.REGISTRY_FILE)
                        if r.get("anchor_id") == anchor["id"] and r.get("status") == "confirmed"]
    assert len(confirmed_events) == 1, f"同一锚点出现 {len(confirmed_events)} 份确认归属"
    assert bindings.latest_confirmed_version(anchor["id"]) == binding_version

    # 编排计划：提交校验后只引用完整绑定版本
    submitted, sub_msg = sched_svc.run_action(schedule["id"], "提交校验")
    assert submitted is not None, f"绑定补齐后应可继续编排: {sub_msg}"
    assert submitted["binding_status"] == "完整"
    assert submitted["binding_versions"] == [binding_version]

    # 启动运行：批次收到的表达式与快照版本必须和绑定快照一致
    started, start_msg = sched_svc.run_action(schedule["id"], "启动运行")
    assert started is not None, f"完整绑定计划应可启动: {start_msg}"
    assert started["status"] == "运行中"

    batches = [b for b in store.rows("batch_queue") if b.get("关联计划") == schedule["计划编号"]]
    assert batches, "启动后未生成采集批次"
    batch = batches[-1]
    assert batch["binding_status"] == "完整"
    assert batch["binding_versions"] == [binding_version]
    ref = batch["binding_refs"][0]
    snapshot = bindings.load_binding(binding_version)
    assert ref["css_expr"] == snapshot["css_expr"] == "div.content > span.name"
    assert ref["xpath_expr"] == snapshot["xpath_expr"]
    assert ref["binding_version"] == binding_version
    assert ref["field_bindings"][0]["field"] == "name"

    # 批次绑定清单独立落档，直接核对
    manifest = persist.read_json(bindings.BINDING_SUB, f"batch_{batch['批次编号']}.json")
    assert manifest is not None
    assert manifest["binding_versions"] == [binding_version]
    assert manifest["binding_refs"][0]["css_expr"] == "div.content > span.name"
    print("PASS: 场景2/3 中断恢复后启动，批次表达式与快照版本核对一致，并发重试无重复定位")


def test_expression_change_keeps_old_plan_binding():
    """场景4：更换定位表达式后，旧计划仍回查自己的原绑定，新版本另立绑定。"""
    script_code = _code("BEHA", "change")
    anchor_code = _code("ELEM", "change")

    script_svc = BehaviorScriptService()
    sel_svc = ElementSelectorService()
    sched_svc = ScriptScheduleService()

    script, _ = script_svc.create_entry({
        "脚本编号": script_code,
        "目标站点": "change.example.com",
        "录制步骤": "open",
    })
    script_svc.run_action(script["id"], "标记生效")
    extract_svc = DataExtractService()
    template, _ = extract_svc.create_entry({
        "模板编号": _code("DATA", "change"),
        "关联脚本": script_code,
        "抽取字段": "title",
    })
    extract_svc.run_action(template["id"], "标记生效")

    anchor, _ = sel_svc.create_entry({
        "锚点编号": anchor_code,
        "关联脚本": script_code,
        "选择器类型": "CSS",
        "CSS表达式": ".old-selector",
    })
    confirmed, _ = sel_svc.run_action(anchor["id"], "标记生效")
    old_version = confirmed["binding_version"]

    schedule = _find_schedule(script_code)
    sched_svc.run_action(schedule["id"], "提交校验")
    assert schedule["binding_versions"] == [old_version]

    # 有意更换定位表达式并重新确认 → 生成新绑定版本
    anchor_row = store.find("element_selector", anchor["id"])
    anchor_row["CSS表达式"] = ".new-selector"
    reconfirmed, _ = sel_svc.run_action(anchor["id"], "标记生效")
    new_version = reconfirmed["binding_version"]
    assert new_version != old_version

    # 旧计划不漂移：仍引用并能回查原绑定表达式
    assert schedule["binding_status"] == "完整"
    assert schedule["binding_versions"] == [old_version]
    old_snapshot = bindings.load_binding(old_version)
    assert old_snapshot is not None and old_snapshot["css_expr"] == ".old-selector"
    new_snapshot = bindings.load_binding(new_version)
    assert new_snapshot is not None and new_snapshot["css_expr"] == ".new-selector"

    # 锚点历史绑定版本均可回查，当前可执行定位只有新版本
    assert set(bindings.binding_versions_for_anchor(anchor["id"])) == {old_version, new_version}
    assert bindings.latest_confirmed_version(anchor["id"]) == new_version
    print("PASS: 场景4 更换表达式后旧计划保留原绑定，新旧版本均可回查")


if __name__ == "__main__":
    test_anchor_on_missing_script_is_rejected_and_auditable()
    test_cross_activation_retry_with_interrupted_persistence()
    test_expression_change_keeps_old_plan_binding()
    print("\nALL E2E BINDING TESTS PASSED")
