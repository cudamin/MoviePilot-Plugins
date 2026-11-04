"""OpenAI Chat Completions 与宿主 LangChain 消息之间的无损边界。"""

from __future__ import annotations

import json
import math
from copy import deepcopy
from dataclasses import dataclass
from typing import Any


class ProtocolError(ValueError):
    """
    面向客户端的确定性协议错误，消息不包含原始请求或凭据。
    """

    def __init__(self, message: str, *, code="invalid_request", status=400):
        super().__init__(message)
        self.code, self.status = code, status


@dataclass
class ChatRequest:
    """
    已验证的客户端请求和转换后的消息。

    :param model: 客户端选择的普通模型或永久聚合别名
    :param messages: 保留工具和推理协议的 LangChain 消息
    :param stream: 是否返回 SSE 流
    :param tools: 交给上游绑定的工具描述，不在网关执行
    :param tool_choice: 上游工具选择规则
    :param options: 明确支持的调用参数
    :param include_usage: 是否在流尾附加用量
    """
    model: str
    messages: list[Any]
    stream: bool
    tools: list[dict] | None
    tool_choice: Any
    options: dict[str, Any]
    include_usage: bool


def parse_request(body: Any) -> ChatRequest:
    """
    仅接受网关实际支持的单选择 Chat Completions 请求。
    """
    if not isinstance(body, dict):
        raise ProtocolError("请求必须是 JSON 对象")
    model = body.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ProtocolError("缺少 model")
    raw_messages = body.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise ProtocolError("messages 必须是非空数组")
    if body.get("n", 1) != 1:
        raise ProtocolError("本地网关仅支持 n=1")
    stream = body.get("stream", False)
    if not isinstance(stream, bool):
        raise ProtocolError("stream 必须是布尔值")
    options = {}
    for name in ("temperature", "top_p", "presence_penalty", "frequency_penalty"):
        if name in body and body[name] is not None:
            value = body[name]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ProtocolError(f"{name} 必须是有限数值")
            options[name] = value
    for name in ("max_tokens", "max_completion_tokens"):
        if name in body:
            if isinstance(body[name], bool) or not isinstance(body[name], int) or body[name] < 1:
                raise ProtocolError(f"{name} 必须是正整数")
            options["max_tokens"] = body[name]
    if "stop" in body:
        stop = body["stop"]
        if stop is not None and not isinstance(stop, str) and not (
                isinstance(stop, list) and all(isinstance(item, str) for item in stop)):
            raise ProtocolError("stop 必须是文本或文本数组")
        options["stop"] = stop
    # 这些由各 SDK 的 bind/ainvoke 明确处理；不支持时返回错误，不退回另一协议。
    for name in ("response_format", "seed", "logprobs", "top_logprobs", "parallel_tool_calls"):
        if name in body:
            options[name] = deepcopy(body[name])
    tools = body.get("tools")
    if tools is not None and (not isinstance(tools, list) or not all(isinstance(item, dict) for item in tools)):
        raise ProtocolError("tools 必须是对象数组")
    tool_choice = body.get("tool_choice")
    if tool_choice is not None and not tools:
        raise ProtocolError("tool_choice 必须同时提供 tools")
    stream_options = body.get("stream_options") or {}
    if not isinstance(stream_options, dict):
        raise ProtocolError("stream_options 必须是对象")
    return ChatRequest(model.strip(), convert_messages(raw_messages), stream,
                       deepcopy(tools), deepcopy(tool_choice), options,
                       bool(stream_options.get("include_usage")))


def convert_messages(messages: list[dict]) -> list[Any]:
    """
    保留转换器遗漏的 reasoning、工具原始参数和供应商签名。
    """
    from langchain_openai.chat_models.base import _convert_dict_to_message

    result = []
    for raw in messages:
        if not isinstance(raw, dict) or raw.get("role") not in {
                "system", "developer", "user", "assistant", "tool", "function"}:
            raise ProtocolError("messages 中存在无效 role")
        content = raw.get("content")
        if content is not None and not isinstance(content, (str, list)):
            raise ProtocolError("message content 必须是文本、数组或 null")
        try:
            message = _convert_dict_to_message(deepcopy(raw))
        except (TypeError, ValueError, KeyError) as error:
            raise ProtocolError("message 格式无效") from error
        for name, value in raw.items():
            if name not in {"role", "content", "name", "id", "tool_call_id"}:
                message.additional_kwargs[name] = deepcopy(value)
        result.append(message)
    return result


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def tool_calls(message: Any, *, streaming=False) -> list[dict]:
    extra = getattr(message, "additional_kwargs", {}) or {}
    if extra.get("tool_calls"):
        calls = deepcopy(extra["tool_calls"])
        if streaming:
            for index, call in enumerate(calls):
                call.setdefault("index", index)
        return calls
    fragments = getattr(message, "tool_call_chunks", None) or extra.get("tool_call_chunks")
    if streaming and fragments:
        result = []
        for index, call in enumerate(fragments):
            entry = {"index": call.get("index") if call.get("index") is not None else index}
            if call.get("id") is not None:
                entry.update(id=call["id"], type="function")
            function = {}
            if call.get("name") is not None:
                function["name"] = call["name"]
            if call.get("args") is not None:
                function["arguments"] = call["args"]
            if function:
                entry["function"] = function
            result.append(entry)
        return result
    result = []
    for index, call in enumerate(getattr(message, "tool_calls", []) or []):
        args = call.get("args", {})
        entry = {"id": call.get("id"), "type": "function", "function": {
            "name": call.get("name", ""),
            "arguments": args if isinstance(args, str) else compact_json(args)}}
        if streaming:
            entry["index"] = index
        result.append(entry)
    return result


def message_payload(message: Any, *, streaming=False) -> dict:
    """
    不把带 thinking/signature 的 content 数组压成纯文本。
    """
    from langchain_core.messages import AIMessage, AIMessageChunk

    if not isinstance(message, (AIMessage, AIMessageChunk)):
        raise ProtocolError("上游 SDK 返回了不支持的消息类型", code="adapter_error", status=502)
    result = {} if streaming else {"role": "assistant"}
    content = deepcopy(message.content)
    if content or not streaming:
        result["content"] = content
    extra = message.additional_kwargs or {}
    for name in ("reasoning_content", "reasoning", "thinking", "refusal", "audio", "function_call"):
        if name in extra:
            result[name] = deepcopy(extra[name])
    calls = tool_calls(message, streaming=streaming)
    if calls:
        result["tool_calls"] = calls
        if not streaming and not content:
            result["content"] = None
    return result


def finish_reason(message: Any) -> str | None:
    metadata = getattr(message, "response_metadata", {}) or {}
    reason = metadata.get("finish_reason") or metadata.get("stop_reason")
    return {"end_turn": "stop", "stop_sequence": "stop", "tool_use": "tool_calls",
            "max_tokens": "length"}.get(reason, reason)


def usage_payload(message: Any) -> dict | None:
    metadata = getattr(message, "response_metadata", {}) or {}
    raw = metadata.get("token_usage") or metadata.get("usage")
    if isinstance(raw, dict) and "prompt_tokens" in raw:
        return {key: deepcopy(value) for key, value in raw.items() if value is not None}
    usage = getattr(message, "usage_metadata", None)
    if not usage:
        return None
    result = {"prompt_tokens": usage.get("input_tokens", 0),
              "completion_tokens": usage.get("output_tokens", 0),
              "total_tokens": usage.get("total_tokens", 0)}
    for source, target in (("input_token_details", "prompt_tokens_details"),
                           ("output_token_details", "completion_tokens_details")):
        details = usage.get(source)
        if details:
            result[target] = {
                {"cache_read": "cached_tokens", "cache_creation": "cache_creation_tokens",
                 "reasoning": "reasoning_tokens", "audio": "audio_tokens"}.get(name, name): value
                for name, value in details.items()}
    return result


def completion(message: Any, *, identifier: str, model: str, created: int) -> dict:
    payload = message_payload(message)
    result = {"id": identifier, "object": "chat.completion", "created": created, "model": model,
              "choices": [{"index": 0, "message": payload,
                           "finish_reason": finish_reason(message) or ("tool_calls" if payload.get("tool_calls") else "stop")}]}
    usage = usage_payload(message)
    if usage is not None:
        result["usage"] = usage
    return result


def stream_event(delta: dict, *, identifier: str, model: str, created: int,
                 reason: str | None = None, usage: dict | None = None) -> str:
    body = {"id": identifier, "object": "chat.completion.chunk", "created": created, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": reason}]}
    if usage is not None:
        body.update(choices=[], usage=usage)
    return f"data: {compact_json(body)}\n\n"


def error_payload(message: str, code: str) -> dict:
    return {"error": {"message": message, "type": "mp_switch_error", "param": None, "code": code}}
