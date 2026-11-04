"""本地 OpenAI 网关的真实 ASGI/loopback 协议回归；上游全部为内存模型。"""

from __future__ import annotations

import asyncio
import json
import socket
from copy import deepcopy
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pytest
from langchain_core.messages import AIMessage, AIMessageChunk


def gateway_class():
    """延迟导入让缺失功能表现为测试失败，而非收集错误。"""
    import agentconfigprofile.gateway as module

    return module.LocalGateway


def candidate(identifier="primary", **overrides):
    configuration = {
        "LLM_PROVIDER": "openai",
        "LLM_MODEL": f"{identifier}-model",
        "LLM_API_KEY": f"sk-{identifier}",
        "LLM_BASE_URL": f"https://{identifier}.example/v1",
        "LLM_BASE_URL_PRESET": None,
        "LLM_USER_AGENT": None,
        "LLM_USE_PROXY": False,
        "LLM_THINKING_LEVEL": "off",
        "LLM_API_PROTOCOL": "chat_completions",
        "LLM_WEB_SEARCH_MODE": "local",
        "LLM_TEMPERATURE": 0.3,
    }
    configuration.update(overrides)
    return {"id": identifier, "name": identifier, "llm": configuration}


class PluginPort:
    def __init__(self, candidates=None):
        self.candidates = candidates if candidates is not None else [candidate()]
        self.records = []
        self.revision = 4
        self.requested_models = []

    def gateway_candidates(self, model):
        self.requested_models.append(model)
        return deepcopy(self.candidates)

    def gateway_record(self, selected, success):
        self.records.append((selected["id"], success, selected["_gateway_revision"]))

    def gateway_mode_revision(self):
        return self.revision

    def gateway_models(self):
        return [{"id": "ccs-a/primary-model", "object": "model", "owned_by": "MP Switch"}]


class MemoryLLM:
    """复现 LangChain 模型异步迭代和独占 transport 的生命周期。"""
    def __init__(self, *, chunks=(), reply=None, wait=None):
        self.chunks = chunks
        self.reply = reply if reply is not None else AIMessage(content="complete")
        self.wait = wait
        self.closed = False
        self.iterator_closed = False
        self.messages = None
        self.options = None
        self.tools = None
        self.tool_choice = None
        self.http_async_client = self

    @property
    def is_closed(self):
        return self.closed

    async def aclose(self):
        self.closed = True

    def bind_tools(self, tools, **options):
        self.tools = deepcopy(tools)
        self.tool_choice = options.get("tool_choice")
        return self

    async def ainvoke(self, messages, **options):
        self.messages, self.options = messages, options
        if self.wait is not None:
            await self.wait()
        if isinstance(self.reply, BaseException):
            raise self.reply
        return self.reply

    async def astream(self, messages, **options):
        self.messages, self.options = messages, options
        try:
            for item in self.chunks:
                if isinstance(item, BaseException):
                    raise item
                yield item
            if self.wait is not None:
                await self.wait()
        finally:
            self.iterator_closed = True


def factory_for(models, calls):
    def factory(selected, options):
        calls.append(selected["id"])
        return models[selected["id"]]
    return factory


async def request(gateway, *, payload=None, authorization=True, path="/v1/chat/completions"):
    headers = {"Authorization": f"Bearer {gateway.api_key}"} if authorization else {}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app),
                                 base_url="http://127.0.0.1", trust_env=False) as client:
        if payload is None:
            return await client.get(path, headers=headers)
        return await client.post(path, json=payload, headers=headers)


def chat_payload(**updates):
    result = {"model": "public-model", "messages": [{"role": "user", "content": "hello"}]}
    result.update(updates)
    return result


def events(response):
    return [json.loads(line[6:]) for line in response.text.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"]


def test_models_accept_only_the_gateway_bearer_token():
    plugin = PluginPort()
    gateway = gateway_class()(plugin)
    unauthorized = asyncio.run(request(gateway, authorization=False, path="/v1/models"))
    assert unauthorized.status_code == 401
    query_token = asyncio.run(request(gateway, authorization=False,
                                     path=f"/v1/models?token={gateway.api_key}"))
    assert query_token.status_code == 401
    async def wrong_token():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app),
                                     base_url="http://127.0.0.1", trust_env=False) as client:
            return await client.get("/v1/models", headers={"Authorization": "Bearer sk-primary"})
    assert asyncio.run(wrong_token()).status_code == 401
    response = asyncio.run(request(gateway, path="/v1/models"))
    assert response.status_code == 200
    assert response.json() == {"object": "list", "data": [
        {"id": "ccs-a/primary-model", "object": "model", "owned_by": "MP Switch"}]}


def test_stream_failure_before_first_chunk_tries_the_next_candidate():
    plugin, calls = PluginPort([candidate(), candidate("backup")]), []
    primary = MemoryLLM(chunks=[RuntimeError("upstream unavailable")])
    backup = MemoryLLM(chunks=[AIMessageChunk(content="from backup")])
    gateway = gateway_class()(plugin, transport_factory=factory_for(
        {"primary": primary, "backup": backup}, calls))
    response = asyncio.run(request(gateway, payload=chat_payload(stream=True)))
    assert response.status_code == 200
    assert [e["choices"][0]["delta"].get("content") for e in events(response)] == ["from backup", None]
    assert response.text.endswith("data: [DONE]\n\n")
    assert calls == ["primary", "backup"]
    assert plugin.records == [("primary", False, 4), ("backup", True, 4)]
    assert primary.closed and primary.iterator_closed and backup.closed


def test_stream_failure_after_first_chunk_emits_error_without_provider_replay():
    plugin, calls = PluginPort([candidate(), candidate("backup")]), []
    primary = MemoryLLM(chunks=[AIMessageChunk(content="already emitted"),
                                RuntimeError("contains sk-primary")])
    gateway = gateway_class()(plugin, transport_factory=factory_for(
        {"primary": primary, "backup": MemoryLLM()}, calls))
    response = asyncio.run(request(gateway, payload=chat_payload(stream=True)))
    output = events(response)
    assert response.status_code == 200
    assert output[0]["choices"][0]["delta"]["content"] == "already emitted"
    assert output[-1]["error"]["code"] == "upstream_stream_error"
    assert "sk-primary" not in response.text
    assert calls == ["primary"]
    assert plugin.records == [("primary", False, 4)]
    assert primary.closed and primary.iterator_closed


def test_nonstream_preserves_tool_reasoning_blocks_and_usage():
    thinking = {"type": "thinking", "thinking": "private thought", "signature": "opaque-sig"}
    reply = AIMessage(content=[thinking, {"type": "text", "text": "ready"}],
                      additional_kwargs={"reasoning_content": "reasoned", "audio": {"id": "audio-1"}},
                      tool_calls=[{"id": "call1", "name": "lookup", "args": {"query": "x"}}],
                      usage_metadata={"input_tokens": 7, "output_tokens": 3, "total_tokens": 10,
                                      "output_token_details": {"reasoning": 2}},
                      response_metadata={"finish_reason": "tool_calls"})
    model, plugin = MemoryLLM(reply=reply), PluginPort()
    gateway = gateway_class()(plugin, transport_factory=lambda *_: model)
    response = asyncio.run(request(gateway, payload=chat_payload()))
    body = response.json()
    assert response.status_code == 200
    assert body["model"] == "public-model"
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["finish_reason"] == "tool_calls"
    message = body["choices"][0]["message"]
    assert message["content"] == [thinking, {"type": "text", "text": "ready"}]
    assert message["reasoning_content"] == "reasoned"
    assert message["audio"] == {"id": "audio-1"}
    assert message["tool_calls"] == [{"id": "call1", "type": "function", "function": {
        "name": "lookup", "arguments": '{"query":"x"}'}}]
    assert body["usage"] == {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10,
                             "completion_tokens_details": {"reasoning_tokens": 2}}
    assert model.closed


def test_stream_preserves_tool_argument_fragments_reasoning_and_usage_tail():
    model = MemoryLLM(chunks=[
        AIMessageChunk(content="", additional_kwargs={"reasoning_content": "thinking"},
                       tool_call_chunks=[{"id": "call1", "index": 0, "name": "lookup", "args": '{"q":'}]),
        AIMessageChunk(content="", tool_call_chunks=[{"id": None, "index": 0,
                                                       "name": None, "args": '"x"}'}]),
        AIMessageChunk(content="", response_metadata={"finish_reason": "tool_calls"},
                       usage_metadata={"input_tokens": 8, "output_tokens": 2, "total_tokens": 10})])
    gateway = gateway_class()(PluginPort(), transport_factory=lambda *_: model)
    response = asyncio.run(request(gateway, payload=chat_payload(
        stream=True, stream_options={"include_usage": True})))
    output = events(response)
    assert output[0]["choices"][0]["delta"]["reasoning_content"] == "thinking"
    assert output[0]["choices"][0]["delta"]["tool_calls"] == [{"index": 0, "id": "call1",
        "type": "function", "function": {"name": "lookup", "arguments": '{"q":'}}]
    assert output[1]["choices"][0]["delta"]["tool_calls"] == [{"index": 0,
        "function": {"arguments": '"x"}'}}]
    assert output[-2]["choices"][0]["finish_reason"] == "tool_calls"
    assert output[-1]["choices"] == []
    assert output[-1]["usage"] == {"prompt_tokens": 8, "completion_tokens": 2, "total_tokens": 10}
    assert sum(bool(e.get("choices") and e["choices"][0]["finish_reason"]) for e in output) == 1


def test_request_retains_assistant_reasoning_and_translates_tool_options():
    model = MemoryLLM()
    gateway = gateway_class()(PluginPort(), transport_factory=lambda *_: model)
    tool = {"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}
    response = asyncio.run(request(gateway, payload=chat_payload(messages=[
        {"role": "developer", "content": "Use tools"},
        {"role": "assistant", "content": [{"type": "thinking", "thinking": "thought", "signature": "sig"}],
         "reasoning_content": "reasoned", "tool_calls": [{"id": "call1", "type": "function",
           "function": {"name": "lookup", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call1", "content": "result"}],
        tools=[tool], tool_choice="auto", stop=["STOP"], max_tokens=99, temperature=0.2)))
    assert response.status_code == 200
    assert model.messages[0].additional_kwargs["__openai_role__"] == "developer"
    assert model.messages[1].additional_kwargs["reasoning_content"] == "reasoned"
    assert model.messages[1].additional_kwargs["tool_calls"][0]["id"] == "call1"
    assert model.messages[1].content[0]["signature"] == "sig"
    assert model.messages[2].tool_call_id == "call1"
    assert model.tools == [tool] and model.tool_choice == "auto"
    assert model.options == {"stop": ["STOP"], "max_tokens": 99, "temperature": 0.2}


@pytest.mark.parametrize("stream", [True, False])
def test_cancellation_before_output_is_neutral_and_closes_owned_clients(stream):
    async def scenario():
        entered = asyncio.Event()
        async def wait():
            entered.set()
            await asyncio.Event().wait()
        model, plugin = MemoryLLM(wait=wait), PluginPort([candidate(), candidate("backup")])
        calls = []
        gateway = gateway_class()(plugin, transport_factory=factory_for(
            {"primary": model, "backup": MemoryLLM()}, calls))
        task = asyncio.create_task(request(gateway, payload=chat_payload(stream=stream)))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert plugin.records == [("primary", None, 4)]
        assert calls == ["primary"]
        assert model.closed
        if stream:
            assert model.iterator_closed
    asyncio.run(scenario())


def test_completed_request_carries_its_original_mode_revision():
    plugin = PluginPort()
    async def change_mode():
        plugin.revision += 1
    model = MemoryLLM(wait=change_mode)
    gateway = gateway_class()(plugin, transport_factory=lambda *_: model)
    assert asyncio.run(request(gateway, payload=chat_payload())).status_code == 200
    assert plugin.records == [("primary", True, 4)]


@pytest.mark.parametrize("bad_config", [{"LLM_API_KEY": ""}, {"LLM_BASE_URL": "http://localhost:0/v1"}])
def test_missing_key_and_self_target_never_construct_an_upstream(bad_config):
    selected = candidate(**bad_config)
    plugin, calls = PluginPort([selected]), []
    gateway = gateway_class()(plugin, transport_factory=factory_for({"primary": MemoryLLM()}, calls))
    if bad_config.get("LLM_BASE_URL"):
        selected["llm"]["LLM_BASE_URL"] = gateway.base_url
    response = asyncio.run(request(gateway, payload=chat_payload()))
    assert response.status_code == 502
    assert calls == []


def test_all_failed_candidates_return_bounded_redacted_error():
    plugin = PluginPort([candidate(str(index)) for index in range(10)])
    calls = []
    models = {str(index): MemoryLLM(reply=RuntimeError(f"bad sk-{index}")) for index in range(10)}
    gateway = gateway_class()(plugin, transport_factory=factory_for(models, calls))
    response = asyncio.run(request(gateway, payload=chat_payload()))
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert len(calls) == 8
    assert "sk-" not in response.text
    assert all(model.closed for key, model in models.items() if int(key) < 8)


def test_unknown_model_and_unsupported_multichoice_return_explicit_errors():
    gateway = gateway_class()(PluginPort([]))
    response = asyncio.run(request(gateway, payload=chat_payload()))
    assert response.status_code == 404
    response = asyncio.run(request(gateway, payload=chat_payload(n=2)))
    assert response.status_code == 400


def test_body_and_concurrent_request_limits_reject_before_upstream():
    async def scenario():
        entered, released = asyncio.Event(), asyncio.Event()
        async def wait():
            entered.set()
            await released.wait()
        model = MemoryLLM(wait=wait)
        gateway = gateway_class()(PluginPort(), transport_factory=lambda *_: model,
                                  max_requests=1, max_body_bytes=512)
        oversized = await request(gateway, payload=chat_payload(messages=[{"role": "user", "content": "x" * 600}]))
        assert oversized.status_code == 413
        first = asyncio.create_task(request(gateway, payload=chat_payload()))
        await asyncio.wait_for(entered.wait(), 2)
        try:
            second = await request(gateway, payload=chat_payload())
            assert second.status_code == 429
        finally:
            released.set()
            await first
    asyncio.run(scenario())


def test_default_factory_pins_every_setting_and_disables_sdk_retries(monkeypatch):
    from app.agent.llm.helper import LLMHelper
    captured = {}
    model = MemoryLLM()
    model.max_retries = 3
    model.request_timeout = None
    model.root_client = SimpleNamespace(max_retries=3, timeout=None)
    model.root_async_client = SimpleNamespace(max_retries=3, timeout=None)
    async def get_llm(**options):
        captured.update(options)
        return model
    monkeypatch.setattr(LLMHelper, "get_llm", get_llm)
    gateway = gateway_class()(PluginPort())
    response = asyncio.run(request(gateway, payload=chat_payload()))
    assert response.status_code == 200
    for key, value in {"streaming": False, "provider": "openai", "model": "primary-model",
                       "api_key": "sk-primary", "base_url": "https://primary.example/v1",
                       "base_url_preset": "", "user_agent": "", "use_proxy": False,
                       "thinking_level": "off", "api_protocol": "chat_completions",
                       "web_search_mode": "local", "temperature": 0.3,
                       "prompt_cache_key": ""}.items():
        assert key in captured and captured[key] == value
    assert model.max_retries == 0 and model.root_client.max_retries == 0
    assert model.root_async_client.max_retries == 0
    assert 20 <= model.request_timeout <= 120
    assert model.root_async_client.timeout == model.request_timeout
    assert model.closed


def test_start_returns_ready_loopback_and_stop_releases_the_listener():
    gateway = gateway_class()(PluginPort(), transport_factory=lambda *_: MemoryLLM())
    first = gateway.start()
    try:
        assert first["base_url"].startswith("http://127.0.0.1:")
        assert first["base_url"].endswith("/v1")
        assert gateway.start() == first
        with httpx.Client(trust_env=False, timeout=2) as client:
            response = client.get(f'{first["base_url"]}/models',
                                  headers={"Authorization": f'Bearer {first["api_key"]}'})
        assert response.status_code == 200
    finally:
        gateway.stop()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", urlsplit(first["base_url"]).port))
    second = gateway.start()
    try:
        assert second["api_key"] != first["api_key"]
    finally:
        gateway.stop()


def test_half_open_denied_candidate_does_not_build_or_record_an_upstream():
    plugin, calls = PluginPort([candidate(), candidate("backup")]), []
    plugin.gateway_permit = lambda selected: selected["id"] != "primary"
    gateway = gateway_class()(plugin, transport_factory=factory_for(
        {"primary": MemoryLLM(), "backup": MemoryLLM()}, calls))
    assert asyncio.run(request(gateway, payload=chat_payload())).status_code == 200
    assert calls == ["backup"]
    assert plugin.records == [("backup", True, 4)]


def test_client_disconnect_after_first_event_is_neutral_and_closes_stream():
    async def scenario():
        entered = asyncio.Event()
        async def wait():
            entered.set()
            await asyncio.Event().wait()
        model = MemoryLLM(chunks=[AIMessageChunk(content="first")], wait=wait)
        plugin, calls = PluginPort([candidate(), candidate("backup")]), []
        gateway = gateway_class()(plugin, transport_factory=factory_for(
            {"primary": model, "backup": MemoryLLM()}, calls))
        output = []
        body = json.dumps(chat_payload(stream=True)).encode()
        supplied = False
        async def receive():
            nonlocal supplied
            if not supplied:
                supplied = True
                return {"type": "http.request", "body": body, "more_body": False}
            await entered.wait()
            return {"type": "http.disconnect"}
        async def send(message):
            output.append(message)
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
                 "http_version": "1.1", "method": "POST", "scheme": "http",
                 "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions",
                 "query_string": b"", "headers": [(b"content-type", b"application/json"),
                 (b"authorization", f"Bearer {gateway.api_key}".encode())],
                 "server": ("127.0.0.1", 1), "client": ("127.0.0.1", 2), "root_path": ""}
        await asyncio.wait_for(gateway.app(scope, receive, send), 3)
        assert b"first" in b"".join(item.get("body", b"") for item in output)
        assert calls == ["primary"]
        assert plugin.records == [("primary", None, 4)]
        assert model.iterator_closed and model.closed
    asyncio.run(scenario())


def test_real_openai_sdk_fails_once_and_uses_candidate_model_and_message_fields(monkeypatch):
    from app.agent.llm.helper import LLMHelper
    from langchain_openai import ChatOpenAI
    outgoing, models = [], []
    async def get_llm(**options):
        def send_upstream(request):
            outgoing.append((request.url.host, json.loads(request.content)))
            if request.url.host == "primary.example":
                return httpx.Response(503, json={"error": {"message": "temporary unavailable"}})
            return httpx.Response(200, json={"id": "chatcmpl-backup", "object": "chat.completion",
                "created": 1, "model": "backup-model", "choices": [{"index": 0,
                "message": {"role": "assistant", "content": "backup response"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}})
        transport = httpx.MockTransport(send_upstream)
        model = ChatOpenAI(model=options["model"], api_key=options["api_key"], base_url=options["base_url"],
                           max_retries=3, http_socket_options=(),
                           http_client=httpx.Client(transport=transport, trust_env=False),
                           http_async_client=httpx.AsyncClient(transport=transport, trust_env=False))
        models.append(model)
        return model
    monkeypatch.setattr(LLMHelper, "get_llm", get_llm)
    plugin = PluginPort([candidate(), candidate("backup")])
    gateway = gateway_class()(plugin)
    response = asyncio.run(request(gateway, payload=chat_payload(stop=["STOP"], max_tokens=41, temperature=0.2)))
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == "backup response"
    assert response.json()["usage"] == {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
    assert [host for host, _body in outgoing] == ["primary.example", "backup.example"]
    assert outgoing[1][1]["model"] == "backup-model"
    assert outgoing[1][1]["messages"] == [{"role": "user", "content": "hello"}]
    assert outgoing[1][1]["stop"] == ["STOP"] and outgoing[1][1]["temperature"] == 0.2
    assert outgoing[1][1].get("max_tokens", outgoing[1][1].get("max_completion_tokens")) == 41
    assert all(model.http_async_client.is_closed and model.http_client.is_closed for model in models)


def test_deleted_stack_model_returns_model_not_found_without_default_fallback():
    plugin = PluginPort()
    def invalid_model(_model):
        raise ValueError("聚合模型所属供应商已移除或模型标识无效")
    plugin.gateway_candidates = invalid_model
    calls = []
    gateway = gateway_class()(plugin, transport_factory=factory_for({"primary": MemoryLLM()}, calls))
    response = asyncio.run(request(gateway, payload=chat_payload(model="ccs-removed/model")))
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "model_not_found"
    assert calls == []


@pytest.mark.parametrize("fail_on", ["http.response.start", "http.response.body"])
def test_disconnected_socket_before_first_body_releases_the_selected_stream(fail_on):
    from starlette.requests import ClientDisconnect
    async def scenario():
        model = MemoryLLM(chunks=[AIMessageChunk(content="first")])
        plugin = PluginPort()
        gateway = gateway_class()(plugin, transport_factory=lambda *_: model)
        body = json.dumps(chat_payload(stream=True)).encode()
        supplied = False
        async def receive():
            nonlocal supplied
            if not supplied:
                supplied = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()
        async def send(message):
            if message["type"] == fail_on:
                raise OSError("socket closed")
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http",
                 "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions",
                 "query_string": b"", "headers": [(b"content-type", b"application/json"),
                 (b"authorization", f"Bearer {gateway.api_key}".encode())],
                 "server": ("127.0.0.1", 1), "client": ("127.0.0.1", 2), "root_path": ""}
        with pytest.raises(ClientDisconnect):
            await gateway.app(scope, receive, send)
        assert plugin.records == [("primary", None, 4)]
        assert model.iterator_closed and model.closed
    asyncio.run(scenario())


def test_real_anthropic_sdk_keeps_thinking_signature_and_closes_its_owned_client(monkeypatch):
    from app.agent.llm import helper
    from app.agent.llm.helper import LLMHelper
    from langchain_anthropic import ChatAnthropic
    models, clients, outbound = [], [], []
    def build_transport(proxy_url, *, async_client=False, timeout=None):
        assert async_client and proxy_url is None
        def send(request):
            outbound.append(json.loads(request.content))
            return httpx.Response(200, json={"id": "msg-test", "type": "message", "role": "assistant",
                "model": "claude-test", "content": [{"type": "thinking", "thinking": "thought",
                "signature": "original-sig"}, {"type": "text", "text": "answer"}],
                "stop_reason": "end_turn", "stop_sequence": None,
                "usage": {"input_tokens": 8, "output_tokens": 4}})
        client = httpx.AsyncClient(transport=httpx.MockTransport(send), trust_env=False, timeout=timeout)
        clients.append(client)
        return client
    async def get_llm(**options):
        model = ChatAnthropic(model=options["model"], api_key=options["api_key"],
                              base_url=options["base_url"], max_retries=3)
        models.append(model)
        return model
    monkeypatch.setattr(LLMHelper, "get_llm", get_llm)
    monkeypatch.setattr(helper, "_build_httpx_client", build_transport)
    gateway = gateway_class()(PluginPort([candidate(LLM_PROVIDER="anthropic", LLM_MODEL="claude-test")]))
    response = asyncio.run(request(gateway, payload=chat_payload()))
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == [
        {"type": "thinking", "thinking": "thought", "signature": "original-sig"},
        {"type": "text", "text": "answer"}]
    assert outbound[0]["model"] == "claude-test"
    assert models[0]._async_client.max_retries == 0
    assert models[0]._async_client.timeout == models[0].default_request_timeout
    assert clients[0].is_closed


def test_anthropic_does_not_close_the_hosts_cached_shared_transport(monkeypatch):
    import anthropic
    from app.agent.llm import helper
    from langchain_anthropic import ChatAnthropic
    created = []
    def reply(_request):
        return httpx.Response(200, json={"id": "msg-test", "type": "message", "role": "assistant",
            "model": "claude-test", "content": [{"type": "text", "text": "own transport"}],
            "stop_reason": "end_turn", "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1}})
    shared = httpx.AsyncClient(transport=httpx.MockTransport(reply), trust_env=False)
    cached = anthropic.AsyncAnthropic(api_key="sk-test", http_client=shared)
    def build_owned(proxy_url, *, async_client=False, timeout=None):
        assert async_client and proxy_url is None
        client = httpx.AsyncClient(transport=httpx.MockTransport(reply), trust_env=False, timeout=timeout)
        created.append(client)
        return client
    async def get_llm(**options):
        model = ChatAnthropic(model=options["model"], api_key=options["api_key"],
                              base_url=options["base_url"], max_retries=3)
        model.__dict__["_async_client"] = cached
        return model
    monkeypatch.setattr(helper.LLMHelper, "get_llm", get_llm)
    monkeypatch.setattr(helper, "_build_httpx_client", build_owned)
    gateway = gateway_class()(PluginPort([candidate(LLM_PROVIDER="anthropic", LLM_MODEL="claude-test")]))
    try:
        assert asyncio.run(request(gateway, payload=chat_payload())).status_code == 200
        assert not shared.is_closed
        assert len(created) == 1 and created[0].is_closed
    finally:
        asyncio.run(shared.aclose())
