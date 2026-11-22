"""脚本调度领域服务：消费behavior_script脚本，创建调度计划、驱动batch_queue执行。

修复点：
- 计划编排（提交校验）只引用已完整持久化的锚点绑定版本，未确认表达式不进入计划。
- 启动运行前再次核验绑定快照仍可回查，无完整绑定的计划拒绝启动。
- 启动后生成的采集批次携带绑定版本与定位表达式清单，批次侧可直接核对快照版本。
- 已持完整绑定的计划不随锚点新版本漂移，旧计划始终能回查自己的绑定。
"""
from __future__ import annotations

from typing import Any

from app import bindings
from app import persistence as persist
from app.services.base import BaseModuleService, register_hook
from app.store import store


def _create_batch_on_start(entry: dict[str, Any]) -> None:
    """调度计划启动运行时，自动创建batch_queue批次并下发绑定版本与定位表达式。"""
    ok, reason = bindings.verify_plan_binding(entry)
    if not ok:
        # 双保险：正常流程已在 run_action 闸门拦截，钩子内再次拒绝孤立定位。
        entry["abnormal"] = True
        persist.append_jsonl("runs", "batch_queue.jsonl", {
            "module": "batch_queue", "entity": "采集批次", "entry_id": None,
            "status": "拒绝创建", "version": "", "action": "startup_denied_missing_binding",
            "timestamp": persist.now_iso(), "abnormal": True,
            "source_module": "script_schedule", "source_id": entry.get("id"),
            "reason": reason,
        })
        return

    batch_rows = store.rows("batch_queue")
    new_id = max((int(r.get("id", 0)) for r in batch_rows), default=0) + 1
    binding_versions = list(entry.get("binding_versions") or [])
    batch = {
        "id": new_id,
        "批次编号": f"BATC-AUTO-{new_id:04d}",
        "关联计划": entry.get("计划编号", ""),
        "任务数量": 10,
        "完成数": "0",
        "失败数": "0",
        "批次状态": "由调度自动生成",
        "截止时间": persist.now_iso(),
        "优先级": "中",
        "status": "排队中",
        "pending": True,
        "abnormal": False,
        # 采集批次只接收完整绑定的版本与表达式，杜绝未确认定位进入采集。
        "binding_status": "完整",
        "binding_versions": binding_versions,
        "binding_refs": list(entry.get("binding_refs") or []),
        "version": "",
        "updated_at": persist.now_iso(),
    }
    batch_rows.append(batch)
    persist.append_jsonl("runs", "batch_queue.jsonl", {
        "module": "batch_queue", "entity": "采集批次", "entry_id": new_id,
        "status": "排队中", "version": ",".join(binding_versions),
        "action": "auto_create_from_schedule",
        "timestamp": persist.now_iso(), "abnormal": False,
        "source_module": "script_schedule", "source_id": entry.get("id"),
    })
    # 批次绑定清单独立原子落档，运行后可直接核对批次收到的表达式与快照版本。
    bindings.write_batch_manifest(batch, entry)


register_hook("script_schedule", "启动运行", _create_batch_on_start)


class ScriptScheduleService(BaseModuleService):
    MODULE = "script_schedule"
    ENTITY = "调度计划"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["计划编号", "关联脚本", "触发条件", "执行间隔", "重跑策略", "依赖任务", "超时控制", "计划状态"]
    REQUIRED_FIELDS = ["计划编号", "关联脚本", "触发条件"]
    STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "启动运行": "运行中", "暂停调度": "已暂停"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "提交校验":
            # 编排锚点绑定：只把完整持久化的绑定版本引用进计划；不完整则留在待补齐范围。
            result = bindings.attach_plan_bindings(entry)
            entry["last_orchestration"] = result["reason"]
            entry["updated_at"] = persist.now_iso()
            if not result["complete"]:
                entry["abnormal"] = True
                self._persist_to_jsonl(entry, action="orchestrate_incomplete")
                return None, result["reason"]
            entry["abnormal"] = False

        if action == "启动运行":
            # 启动硬闸门：只有持完整绑定且快照仍可回查的计划才把版本与表达式交给批次。
            ok, reason = bindings.verify_plan_binding(entry)
            if not ok:
                entry["abnormal"] = True
                entry["last_action"] = action
                entry["updated_at"] = persist.now_iso()
                self._persist_to_jsonl(entry, action="startup_denied")
                return None, reason

        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 关联的batch_queue统计
        batches = store.rows("batch_queue")
        schedules = store.rows(self.MODULE)
        plan_ids = {r.get("计划编号") for r in schedules}
        base["downstream_batches"] = sum(1 for b in batches if b.get("关联计划") in plan_ids)
        # 绑定编排可观测性
        base["plans_with_complete_binding"] = sum(
            1 for r in schedules if r.get("binding_status") == "完整")
        base["plans_unbound"] = sum(
            1 for r in schedules if r.get("binding_status") != "完整")
        base["unconfirmed_binding_events"] = len(bindings.unconfirmed_records())
        # 关联的error_recover任务
        errors = store.rows("error_recover")
        base["recovery_tasks"] = sum(1 for e in errors if e.get("关联计划") in plan_ids)
        return base
