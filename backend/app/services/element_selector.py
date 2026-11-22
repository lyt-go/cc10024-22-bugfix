"""元素锚点领域服务：Web元素定位、选择器校验，消费behavior_script的脚本配置，为data_extract提供定位锚点。

修复点：
- 关联脚本不存在/未生效、或缺少有效采集字段绑定时，「标记生效」必须可查地失败，
  不再把选择器错误显示成已生效。
- 锚点确认改由 app.bindings 编排：绑定版本快照原子落库后才允许进入已生效，
  并发确认按锚点加锁、按内容指纹幂等，重试不产生第二份可执行定位。
"""
from __future__ import annotations

from typing import Any

from app import bindings
from app import persistence as persist
from app.services.base import BaseModuleService
from app.store import store


class ElementSelectorService(BaseModuleService):
    MODULE = "element_selector"
    ENTITY = "元素锚点"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["锚点编号", "关联脚本", "选择器类型", "CSS表达式", "XPath表达式", "文本匹配", "等待条件", "元素顺序"]
    REQUIRED_FIELDS = ["锚点编号", "关联脚本", "选择器类型"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已失效"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "标记生效":
            # 先绑定校验、快照完整落库，再做状态流转；失败时锚点留在可核对范围。
            result = bindings.confirm_selector_binding(entry)
            if not result["ok"]:
                entry["abnormal"] = True
                entry["last_action"] = action
                entry["updated_at"] = persist.now_iso()
                self._persist_to_jsonl(entry, action="binding_rejected")
                return None, result["reason"]
            entry["binding_version"] = result["binding_version"]
            entry["abnormal"] = False

        outcome, message = super().run_action(entry_id, action)
        if outcome is not None and action == "标记生效":
            # 状态流转后回填绑定版本，确保工作台与绑定快照一致。
            outcome["binding_version"] = entry.get("binding_version", "")
        return outcome, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        selectors = store.rows(self.MODULE)
        scripts = store.rows("behavior_script")
        script_ids = {r.get("脚本编号") for r in scripts}
        base["orphan_selectors"] = sum(
            1 for s in selectors if s.get("关联脚本") not in script_ids)
        # 已完整持久化的绑定版本快照数（仅统计有确认归属的快照）。
        confirmed = {
            r.get("binding_version")
            for r in persist.read_jsonl(bindings.BINDING_SUB, bindings.REGISTRY_FILE)
            if r.get("status") == "confirmed" and r.get("binding_version")
        }
        base["selector_snapshots"] = len(confirmed)
        base["confirmed_bindings"] = len(confirmed)
        base["unconfirmed_binding_events"] = len(bindings.unconfirmed_records())
        return base
