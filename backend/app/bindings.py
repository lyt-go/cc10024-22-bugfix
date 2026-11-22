"""采集字段绑定（锚点快照）编排中心。

职责：
1. 元素锚点确认（标记生效）时做脚本与选择器绑定校验：关联脚本必须存在且已生效，
   且脚本至少持有一个已生效的抽取模板（有效采集字段绑定），否则确认必须可查地失败。
2. 绑定通过后生成不可变的「绑定版本」快照并原子落库；同一锚点的并发确认按锚点加锁、
   按内容指纹幂等，重试不会产生第二份可执行定位，落库中断只留下可核对的失败记录。
3. 调度计划只能引用完整持久化的绑定版本；启动运行时再次核验快照仍在，
   未确认/无有效归属的表达式永远不会交给采集批次。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from typing import Any

from app import persistence as persist
from app.store import store

BINDING_SUB = "bindings"
REGISTRY_FILE = "binding_registry.jsonl"

# 进程内故障注入：置位后下一次绑定快照写入被阻断，模拟「锚点快照落库中断」。
_write_failure_armed = threading.Event()


class BindingError(RuntimeError):
    """绑定快照持久化中断等系统级失败。"""


def arm_write_failure() -> None:
    """测试钩子：令下一次绑定快照原子写入抛出异常（模拟落库中断）。"""
    _write_failure_armed.set()


def _canonical(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def binding_fingerprint(selector: dict[str, Any], field_bindings: list[dict[str, Any]]) -> str:
    """定位表达式 + 有效采集字段绑定的内容指纹，内容不变则版本不变（幂等键）。"""
    raw = _canonical({
        "anchor_id": selector.get("id"),
        "selector_type": selector.get("选择器类型"),
        "css_expr": selector.get("CSS表达式"),
        "xpath_expr": selector.get("XPath表达式"),
        "text_match": selector.get("文本匹配"),
        "linked_script": selector.get("关联脚本"),
        "field_bindings": field_bindings,
    })
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _registry_path() -> str:
    return persist.data_path(BINDING_SUB, REGISTRY_FILE)


def _registry_records() -> list[dict[str, Any]]:
    return persist.read_jsonl(BINDING_SUB, REGISTRY_FILE)


def _append_registry(record: dict[str, Any]) -> None:
    persist.append_jsonl(BINDING_SUB, REGISTRY_FILE, record)


def _binding_file(version: str) -> str:
    return f"binding_{version}.json"


def load_binding(version: str) -> dict[str, Any] | None:
    """读取已持久化的绑定快照；文件不存在或未确认归属时返回 None（拒绝孤立定位）。"""
    if not version:
        return None
    payload = persist.read_json(BINDING_SUB, _binding_file(version))
    if payload is None or payload.get("status") != "confirmed":
        return None
    confirmed = any(
        r.get("event") == "confirmed"
        and r.get("binding_version") == version
        and r.get("status") == "confirmed"
        for r in _registry_records()
    )
    return payload if confirmed else None


def latest_confirmed_version(anchor_id: int) -> str | None:
    """取锚点最近一次确认的绑定版本（同一锚点只允许一条当前可执行定位）。"""
    version: str | None = None
    for r in _registry_records():
        if r.get("anchor_id") == anchor_id and r.get("status") == "confirmed":
            version = r.get("binding_version")
    return version


def binding_versions_for_anchor(anchor_id: int) -> list[str]:
    """回查锚点历史上的全部绑定版本（含已被替换的旧版本，供旧计划追溯）。"""
    seen: list[str] = []
    for r in _registry_records():
        if r.get("anchor_id") == anchor_id and r.get("status") == "confirmed":
            v = r.get("binding_version")
            if v and v not in seen:
                seen.append(v)
    return seen


def unconfirmed_records() -> list[dict[str, Any]]:
    """所有无有效归属的快照记录：失败或待确认范围，供工作台核对。"""
    return [r for r in _registry_records() if r.get("status") != "confirmed"]


def _record_event(
    *, event: str, status: str, selector: dict[str, Any], reason: str,
    version: str = "", fingerprint: str = "",
    field_bindings: list[dict[str, Any]] | None = None,
    plan_code: str = "",
) -> dict[str, Any]:
    record = {
        "event": event,
        "status": status,
        "reason": reason,
        "anchor_id": selector.get("id"),
        "anchor_code": selector.get("锚点编号", ""),
        "linked_script": selector.get("关联脚本", ""),
        "binding_version": version,
        "fingerprint": fingerprint,
        "field_bindings": field_bindings or [],
        "plan_code": plan_code,
        "timestamp": persist.now_iso(),
    }
    _append_registry(record)
    return record


def _active_field_bindings(script_code: str) -> list[dict[str, Any]]:
    templates = store.rows("data_extract")
    linked = [
        t for t in templates
        if t.get("关联脚本") == script_code and t.get("status") == "已生效"
    ]
    return [
        {
            "template_id": t.get("id"),
            "template_code": t.get("模板编号", ""),
            "field": t.get("抽取字段", ""),
            "method": t.get("提取方式", ""),
            "output_type": t.get("输出类型", ""),
        }
        for t in linked
    ]


def confirm_selector_binding(selector: dict[str, Any]) -> dict[str, Any]:
    """确认元素锚点绑定。

    返回 ``{"ok": bool, "reason": str, "binding_version": str}``。
    校验失败或落库中断时不改锚点状态，只在绑定登记册留下可核对的失败记录。
    """
    anchor_id = int(selector.get("id", 0))
    # 同一锚点的并发确认/重试串行化，避免重复可执行定位。
    with persist.locking(f"selector_binding:{anchor_id}"):
        script_code = str(selector.get("关联脚本") or "").strip()
        scripts = store.rows("behavior_script")
        linked_scripts = [s for s in scripts if s.get("脚本编号") == script_code]

        if not linked_scripts:
            reason = f"关联脚本「{script_code}」不存在，锚点绑定校验未通过"
            _record_event(event="validate_failed", status="failed",
                          selector=selector, reason=reason)
            return {"ok": False, "reason": reason, "binding_version": ""}

        active_script = next((s for s in linked_scripts if s.get("status") == "已生效"), None)
        if active_script is None:
            reason = f"关联脚本「{script_code}」尚未生效，锚点绑定校验未通过"
            _record_event(event="validate_failed", status="failed",
                          selector=selector, reason=reason)
            return {"ok": False, "reason": reason, "binding_version": ""}

        field_bindings = _active_field_bindings(script_code)
        if not field_bindings:
            reason = (f"脚本「{script_code}」缺少已生效的采集字段绑定（抽取模板），"
                      "锚点不能形成可执行定位")
            _record_event(event="validate_failed", status="failed",
                          selector=selector, reason=reason)
            return {"ok": False, "reason": reason, "binding_version": ""}

        fingerprint = binding_fingerprint(selector, field_bindings)
        version = f"BIND-{fingerprint}"

        # 幂等：内容未变且快照完整持久化时，确认重试直接复用既有版本，不产生第二份定位。
        existing = load_binding(version)
        if existing is not None:
            selector["binding_version"] = version
            selector["binding_fingerprint"] = fingerprint
            return {"ok": True, "reason": "绑定已确认，复用既有绑定版本",
                    "binding_version": version}

        payload = {
            "binding_version": version,
            "fingerprint": fingerprint,
            "status": "confirmed",
            "selector_id": anchor_id,
            "anchor_code": selector.get("锚点编号", ""),
            "selector_type": selector.get("选择器类型", ""),
            "css_expr": selector.get("CSS表达式", ""),
            "xpath_expr": selector.get("XPath表达式", ""),
            "text_match": selector.get("文本匹配", ""),
            "linked_script": script_code,
            "script_active": True,
            "field_bindings": field_bindings,
            "selector_version": selector.get("version", ""),
            "confirmed_at": persist.now_iso(),
            "timestamp": persist.now_iso(),
        }

        try:
            if _write_failure_armed.is_set():
                _write_failure_armed.clear()
                raise BindingError("模拟锚点快照落库中断")
            # 原子替换：要么完整文件可见，要么文件不存在，杜绝半份快照。
            persist.atomic_write_json(BINDING_SUB, _binding_file(version), payload)
            # 落库成功后才登记确认归属；文件与登记册同时成立才算完整绑定。
            _record_event(
                event="confirmed", status="confirmed", selector=selector,
                reason="绑定快照已完整持久化", version=version,
                fingerprint=fingerprint, field_bindings=field_bindings,
            )
        except BindingError as exc:
            _record_event(
                event="persist_failed", status="failed", selector=selector,
                reason=f"锚点快照落库中断：{exc}", version=version,
                fingerprint=fingerprint, field_bindings=field_bindings,
            )
            return {"ok": False,
                    "reason": "锚点快照落库中断，绑定未确认，请重试确认",
                    "binding_version": ""}

        selector["binding_version"] = version
        selector["binding_fingerprint"] = fingerprint
        return {"ok": True, "reason": "锚点绑定已确认并持久化",
                "binding_version": version}


def _confirmed_selectors_for_script(script_code: str) -> list[dict[str, Any]]:
    return [
        s for s in store.rows("element_selector")
        if s.get("关联脚本") == script_code and s.get("status") == "已生效"
    ]


def attach_plan_bindings(schedule: dict[str, Any]) -> dict[str, Any]:
    """为调度计划编排锚点绑定：只引用完整持久化的绑定版本。

    已持完整绑定的计划不随锚点新版本漂移（旧计划保留自己的绑定版本与表达式）。
    返回编排结果摘要；无完整绑定时计划保持未绑定，启动将被阻断。
    """
    plan_code = str(schedule.get("计划编号") or "")
    script_code = str(schedule.get("关联脚本") or "")

    if schedule.get("binding_status") == "完整" and all(
        load_binding(v) for v in schedule.get("binding_versions", [])
    ):
        return {"ok": True, "complete": True, "reason": "计划已持完整绑定，保持既有版本",
                "binding_versions": list(schedule.get("binding_versions", []))}

    with persist.locking(f"plan_binding:{plan_code}"):
        selectors = _confirmed_selectors_for_script(script_code)
        refs: list[dict[str, Any]] = []
        versions: list[str] = []
        for selector in selectors:
            version = latest_confirmed_version(int(selector.get("id", 0)))
            snapshot = load_binding(version) if version else None
            if snapshot is None:
                # 锚点没有有效归属快照：不允许把未确认表达式编进计划。
                _record_event(
                    event="orchestrate_skipped", status="failed",
                    selector=selector,
                    reason=f"计划「{plan_code}」编排时锚点缺少完整绑定快照",
                    plan_code=plan_code,
                )
                schedule["binding_status"] = "未绑定"
                schedule["binding_versions"] = []
                schedule["binding_refs"] = []
                return {"ok": False, "complete": False,
                        "reason": f"锚点「{selector.get('锚点编号', '')}」缺少已确认绑定，计划未取得完整定位",
                        "binding_versions": []}
            refs.append({
                "anchor_id": selector.get("id"),
                "anchor_code": selector.get("锚点编号", ""),
                "binding_version": version,
                "selector_type": snapshot.get("selector_type", ""),
                "css_expr": snapshot.get("css_expr", ""),
                "xpath_expr": snapshot.get("xpath_expr", ""),
                "text_match": snapshot.get("text_match", ""),
                "field_bindings": copy.deepcopy(snapshot.get("field_bindings", [])),
            })
            versions.append(version)  # type: ignore[arg-type]

        if not refs:
            schedule["binding_status"] = "未绑定"
            schedule["binding_versions"] = []
            schedule["binding_refs"] = []
            return {"ok": False, "complete": False,
                    "reason": f"脚本「{script_code}」尚无已确认锚点绑定，计划无可执行定位",
                    "binding_versions": []}

        schedule["binding_status"] = "完整"
        schedule["binding_versions"] = versions
        schedule["binding_refs"] = refs
        return {"ok": True, "complete": True,
                "reason": "计划已绑定完整锚点快照",
                "binding_versions": versions}


def verify_plan_binding(schedule: dict[str, Any]) -> tuple[bool, str]:
    """启动运行闸门：只有持完整绑定且快照仍可持久化回查的计划才能启动。"""
    plan_code = str(schedule.get("计划编号") or "")
    versions = list(schedule.get("binding_versions") or [])
    refs = schedule.get("binding_refs") or []
    if schedule.get("binding_status") != "完整" or not versions or not refs:
        reason = f"计划「{plan_code}」未持完整采集字段绑定，拒绝启动，避免孤立定位执行采集"
        _append_registry({
            "event": "startup_denied", "status": "failed", "reason": reason,
            "anchor_id": None, "anchor_code": "",
            "linked_script": schedule.get("关联脚本", ""),
            "binding_version": "", "fingerprint": "", "field_bindings": [],
            "plan_code": plan_code, "timestamp": persist.now_iso(),
        })
        return False, reason
    for version in versions:
        if load_binding(version) is None:
            reason = f"计划「{plan_code}」引用的绑定版本 {version} 快照缺失或无确认归属，拒绝启动"
            _append_registry({
                "event": "startup_denied", "status": "failed", "reason": reason,
                "anchor_id": None, "anchor_code": "",
                "linked_script": schedule.get("关联脚本", ""),
                "binding_version": version, "fingerprint": "", "field_bindings": [],
                "plan_code": plan_code, "timestamp": persist.now_iso(),
            })
            return False, reason
    return True, "绑定校验通过"


def write_batch_manifest(batch: dict[str, Any], schedule: dict[str, Any]) -> str:
    """把计划持有的绑定版本与定位表达式随采集批次落档，便于直接核对批次收到的内容。"""
    manifest = {
        "batch_code": batch.get("批次编号", ""),
        "plan_code": schedule.get("计划编号", ""),
        "binding_status": schedule.get("binding_status", "未绑定"),
        "binding_versions": list(schedule.get("binding_versions") or []),
        "binding_refs": copy.deepcopy(schedule.get("binding_refs") or []),
        "timestamp": persist.now_iso(),
    }
    return persist.atomic_write_json(
        BINDING_SUB, f"batch_{batch.get('批次编号', 'unknown')}.json", manifest)
