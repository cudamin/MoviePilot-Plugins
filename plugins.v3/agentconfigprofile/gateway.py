"""插件持有的仅 loopback OpenAI Chat Completions 网关。"""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
import logging
import secrets
import socket
import threading
import time
from contextlib import suppress
from urllib.parse import urlsplit
from uuid import uuid4

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .protocol import (
    ProtocolError,
    compact_json,
    completion,
    error_payload,
    finish_reason,
    message_payload,
    parse_request,
    stream_event,
    usage_payload,
)

_logger = logging.getLogger(__name__)


class _RequestBoundary:
    """
    在解析请求和建立上游前统一鉴权、限制正文与并发。
    """

    def __init__(self, app, *, gateway):
        self.app, self.gateway = app, gateway

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        header = next((value for name, value in scope.get("headers", []) if name == b"authorization"), b"")
        prefix, separator, token = header.partition(b" ")
        if prefix.lower() != b"bearer" or not separator or not secrets.compare_digest(
                token, self.gateway.api_key.encode("ascii")):
            response = JSONResponse(error_payload("需要本地网关访问令牌", "invalid_api_key"), 401,
                                    headers={"WWW-Authenticate": "Bearer"})
            return await response(scope, receive, send)
        if not self.gateway._requests.acquire(blocking=False):
            response = JSONResponse(error_payload("本地网关并发请求已达上限", "request_limit"), 429)
            return await response(scope, receive, send)
        try:
            if scope.get("method") == "POST":
                body = bytearray()
                while True:
                    part = await receive()
                    if part["type"] == "http.disconnect":
                        return
                    body.extend(part.get("body", b""))
                    if len(body) > self.gateway.max_body_bytes:
                        response = JSONResponse(error_payload("请求正文过大", "body_limit"), 413)
                        return await response(scope, receive, send)
                    if not part.get("more_body", False):
                        break
                delivered = False
                async def buffered_receive():
                    nonlocal delivered
                    if not delivered:
                        delivered = True
                        return {"type": "http.request", "body": bytes(body), "more_body": False}
                    return await receive()
                await self.app(scope, buffered_receive, send)
            else:
                await self.app(scope, receive, send)
        finally:
            self.gateway._requests.release()


class _OwnedStreamingResponse(StreamingResponse):
    """
    HTTP 头或正文发送失败也必须结算许可并关闭预读的上游。
    """

    def __init__(self, content, *, gateway, selected, model, iterator, outcome):
        super().__init__(content, media_type="text/event-stream",
                         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        self.gateway, self.selected = gateway, selected
        self.model, self.iterator, self.outcome = model, iterator, outcome

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await self.body_iterator.aclose()
                finally:
                    try:
                        self.gateway._record(self.selected, self.outcome[0])
                    finally:
                        await self.gateway._close(self.model, self.iterator)


class LocalGateway:
    """
    真实供应商快照始终只在服务内部使用，不从宿主代理投影继承配置。
    """

    def __init__(self, plugin, *, transport_factory=None, max_requests=16,
                 max_body_bytes=4 * 1024 * 1024, timeout=None):
        self.plugin = plugin
        self.transport_factory = transport_factory or self._create_model
        self.max_body_bytes = max(1, int(max_body_bytes))
        self._requests = threading.BoundedSemaphore(max(1, int(max_requests)))
        self.timeout = self._timeout(timeout if timeout is not None else getattr(plugin, "_test_timeout", 60))
        self.api_key = secrets.token_urlsafe(36)
        self.base_url = "http://127.0.0.1:0/v1"
        self._server = self._thread = self._socket = None
        self._lock = threading.RLock()
        self._ports = {0}
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self.app.add_middleware(_RequestBoundary, gateway=self)
        self.app.add_api_route("/v1/models", self._models, methods=["GET"])
        self.app.add_api_route("/v1/chat/completions", self._chat, methods=["POST"])

    @staticmethod
    def _timeout(value):
        try:
            return min(120.0, max(20.0, float(value)))
        except (TypeError, ValueError):
            return 60.0

    def start(self) -> dict:
        """
        先完成监听和 ASGI 启动，再把地址交给宿主投影。
        """
        import uvicorn

        with self._lock:
            if self._thread and self._thread.is_alive() and self._server.started:
                return {"base_url": self.base_url, "api_key": self.api_key}
            self.stop()
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                listener.bind(("127.0.0.1", 0))
                listener.listen(128)
                listener.setblocking(False)
                port = listener.getsockname()[1]
                self.api_key = secrets.token_urlsafe(36)
                self.base_url = f"http://127.0.0.1:{port}/v1"
                self._ports.add(port)
                config = uvicorn.Config(self.app, host="127.0.0.1", port=port, access_log=False,
                                        log_level="critical", proxy_headers=False, server_header=False,
                                        timeout_graceful_shutdown=2)
                server = uvicorn.Server(config)
                def run():
                    try:
                        server.run(sockets=[listener])
                    except Exception as error:
                        _logger.warning("本地网关监听失败（%s）", type(error).__name__)
                thread = threading.Thread(target=run, name="MP-Switch-Gateway", daemon=True)
                self._socket, self._server, self._thread = listener, server, thread
                thread.start()
                deadline = time.monotonic() + 5
                while not server.started:
                    if not thread.is_alive() or time.monotonic() >= deadline:
                        raise RuntimeError("本地网关未能就绪")
                    time.sleep(0.01)
                return {"base_url": self.base_url, "api_key": self.api_key}
            except BaseException:
                self.stop()
                listener.close()
                raise

    def stop(self):
        """
        等待 uvicorn 取消未完成请求并释放监听套接字。
        """
        with self._lock:
            if self._server is not None:
                self._server.should_exit = True
            thread = self._thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=5)
                if thread.is_alive():
                    self._server.force_exit = True
                    thread.join(timeout=2)
                if thread.is_alive():
                    raise RuntimeError("本地网关关闭超时")
            if self._socket is not None:
                self._socket.close()
            self._socket = self._server = self._thread = None

    async def _models(self):
        return {"object": "list", "data": self.plugin.gateway_models()}

    def _validate_target(self, selected):
        llm = selected.get("llm") or {}
        if not str(llm.get("LLM_API_KEY") or "").strip():
            raise ProtocolError("供应商快照缺少 API Key", code="missing_upstream_key", status=502)
        if llm["LLM_API_KEY"] == self.api_key:
            raise ProtocolError("拒绝将本地网关作为上游", code="recursive_upstream", status=502)
        self._validate_url(str(llm.get("LLM_BASE_URL") or ""))

    def _validate_url(self, value):
        try:
            parsed = urlsplit(value)
            host, port = parsed.hostname or "", parsed.port
        except ValueError as error:
            raise ProtocolError("上游地址无效", code="invalid_upstream_url", status=502) from error
        local = host.lower() in {"localhost", "localhost.localdomain"}
        with suppress(ValueError):
            address = ipaddress.ip_address(host)
            local = address.is_loopback or address.is_unspecified
        if local and port in self._ports:
            raise ProtocolError("拒绝将本地网关作为上游", code="recursive_upstream", status=502)

    async def _create_model(self, selected, options):
        from app.agent.llm.helper import LLMHelper

        llm = selected["llm"]
        if not llm.get("LLM_PROVIDER") or not llm.get("LLM_MODEL"):
            raise ProtocolError("供应商快照缺少供应商或模型", code="invalid_upstream", status=502)
        model = await LLMHelper.get_llm(
            streaming=options["streaming"], provider=llm["LLM_PROVIDER"], model=llm["LLM_MODEL"],
            api_key=llm["LLM_API_KEY"], base_url=llm.get("LLM_BASE_URL") or "",
            base_url_preset=llm.get("LLM_BASE_URL_PRESET") or "",
            user_agent=llm.get("LLM_USER_AGENT") or "", use_proxy=bool(llm.get("LLM_USE_PROXY", False)),
            thinking_level=llm.get("LLM_THINKING_LEVEL") or "off",
            api_protocol=llm.get("LLM_API_PROTOCOL") or "auto",
            web_search_mode=llm.get("LLM_WEB_SEARCH_MODE") or "local",
            temperature=options.get("temperature", llm.get("LLM_TEMPERATURE")), prompt_cache_key="",
            provider_runtime=None)
        try:
            if not hasattr(model, "max_retries"):
                raise ProtocolError("上游 SDK 不支持关闭重试", code="adapter_error", status=502)
            model.max_retries = 0
            timeout_field = "request_timeout" if hasattr(model, "request_timeout") else "default_request_timeout"
            if not hasattr(model, timeout_field):
                raise ProtocolError("上游 SDK 不支持请求超时设置", code="adapter_error", status=502)
            setattr(model, timeout_field, options["timeout"])
            # OpenAI/DeepSeek SDK 在构造时复制重试参数，必须同步修改根客户端。
            for name in ("root_client", "root_async_client"):
                client = getattr(model, name, None)
                if client is not None:
                    client.max_retries, client.timeout = 0, options["timeout"]
                    self._validate_url(str(getattr(client, "base_url", "") or ""))
            from langchain_anthropic import ChatAnthropic
            if isinstance(model, ChatAnthropic):
                # LangChain 默认通过 lru_cache 共用 Anthropic 的 httpx 客户端。
                # 给本请求安装独占 SDK，关闭时不触及宿主或另一请求的缓存。
                import anthropic
                from app.agent.llm.helper import _build_httpx_client, _resolve_llm_proxy

                parameters = dict(model._client_params)
                parameters.update(max_retries=0, timeout=options["timeout"])
                self._validate_url(str(parameters.get("base_url") or ""))
                transport = _build_httpx_client(_resolve_llm_proxy(bool(llm.get("LLM_USE_PROXY", False))),
                                               async_client=True, timeout=options["timeout"])
                try:
                    client = anthropic.AsyncAnthropic(**parameters, http_client=transport)
                except BaseException:
                    await transport.aclose()
                    raise
                model.__dict__["_async_client"] = client
                model.__dict__["_mp_switch_owned_anthropic_client"] = client
            return model
        except BaseException:
            await self._close(model)
            raise

    def _record(self, selected, outcome):
        self.plugin.gateway_record(selected, outcome)

    async def _close(self, model, iterator=None):
        from app.agent.llm.helper import LLMHelper

        # Starlette 的 disconnect 取消作用域不能截断资源收尾。
        with anyio.CancelScope(shield=True):
            if iterator is not None and hasattr(iterator, "aclose"):
                try:
                    await asyncio.wait_for(iterator.aclose(), 3)
                except (Exception, asyncio.CancelledError):
                    _logger.warning("本地网关关闭上游流失败")
            if model is not None:
                try:
                    await asyncio.wait_for(LLMHelper.close_llm(model), 3)
                except (Exception, asyncio.CancelledError):
                    _logger.warning("本地网关关闭上游 transport 失败")
                client = getattr(model, "__dict__", {}).get("_mp_switch_owned_anthropic_client")
                if client is not None and not client.is_closed():
                    try:
                        await asyncio.wait_for(client.close(), 3)
                    except (Exception, asyncio.CancelledError):
                        _logger.warning("本地网关关闭 Anthropic 客户端失败")

    async def _chat(self, request: Request):
        try:
            try:
                payload = parse_request(await request.json())
            except (UnicodeDecodeError, ValueError) as error:
                if isinstance(error, ProtocolError):
                    raise
                raise ProtocolError("请求正文不是有效 JSON") from error
            revision = self.plugin.gateway_mode_revision()
            try:
                candidates = self.plugin.gateway_candidates(payload.model)
            except ValueError as error:
                raise ProtocolError("模型不存在或当前代理模式不可用", code="model_not_found", status=404) from error
            if not candidates:
                raise ProtocolError("模型不存在或无可用供应商", code="model_not_found", status=404)
            identifier, created = f"chatcmpl-{uuid4().hex}", int(time.time())
            deadline = time.monotonic() + self.timeout
            last_error = None
            for source in candidates[:8]:
                selected = {**source, "llm": dict(source.get("llm") or {}), "_gateway_revision": revision}
                permit = getattr(self.plugin, "gateway_permit", None)
                if permit is not None and not permit(selected):
                    continue
                model = iterator = None
                transferred = False
                try:
                    self._validate_target(selected)
                    options = {**payload.options, "streaming": payload.stream, "timeout": self.timeout}
                    factory_result = self.transport_factory(selected, options)
                    remaining = max(0.001, deadline - time.monotonic())
                    model = await asyncio.wait_for(factory_result, remaining) if inspect.isawaitable(factory_result) else factory_result
                    if model is None:
                        raise ProtocolError("上游 SDK 未创建模型", code="adapter_error", status=502)
                    runnable = model
                    if payload.tools:
                        bind = getattr(model, "bind_tools", None)
                        if not callable(bind):
                            raise ProtocolError("上游 SDK 不支持工具调用", code="adapter_error", status=502)
                        bind_options = {"tool_choice": payload.tool_choice} if payload.tool_choice is not None else {}
                        runnable = bind(payload.tools, **bind_options)
                    if payload.stream:
                        iterator = runnable.astream(payload.messages, **payload.options).__aiter__()
                        first = await asyncio.wait_for(anext(iterator), max(0.001, deadline - time.monotonic()))
                        first_delta = message_payload(first, streaming=True)
                        # 在返回 HTTP 200 前真正读到首个消息，并验证其可序列化。
                        compact_json(first_delta)
                        outcome = [None]
                        response = _OwnedStreamingResponse(self._stream(first, first_delta, iterator, payload,
                                                           identifier, created, deadline, outcome),
                                                           gateway=self, selected=selected, model=model,
                                                           iterator=iterator, outcome=outcome)
                        transferred = True
                        return response
                    reply = await asyncio.wait_for(runnable.ainvoke(payload.messages, **payload.options),
                                                   max(0.001, deadline - time.monotonic()))
                    response = JSONResponse(completion(reply, identifier=identifier, model=payload.model, created=created))
                    self._record(selected, True)
                    return response
                except asyncio.CancelledError:
                    self._record(selected, None)
                    raise
                except Exception as error:
                    self._record(selected, False)
                    last_error = error
                    if time.monotonic() >= deadline:
                        break
                finally:
                    if not transferred:
                        await self._close(model, iterator)
            if isinstance(last_error, ProtocolError):
                raise last_error
            raise ProtocolError("上游调用失败或超时", code="upstream_error", status=502)
        except ProtocolError as error:
            return JSONResponse(error_payload(str(error), error.code), error.status)
        except asyncio.CancelledError:
            raise
        except Exception:
            return JSONResponse(error_payload("网关无法解析供应商或适配请求", "gateway_error"), 502)

    async def _stream(self, first, first_delta, iterator, payload, identifier, created, deadline, outcome):
        usage = None
        finished = False
        try:
            chunk, delta, is_first = first, first_delta, True
            while True:
                reason = finish_reason(chunk)
                current_usage = usage_payload(chunk)
                if current_usage is not None:
                    usage = current_usage
                if is_first:
                    delta = {"role": "assistant", **delta}
                if delta or reason:
                    yield stream_event(delta, identifier=identifier, model=payload.model, created=created,
                                       reason=reason if not finished else None)
                finished = finished or bool(reason)
                try:
                    chunk = await asyncio.wait_for(anext(iterator), max(0.001, deadline - time.monotonic()))
                except StopAsyncIteration:
                    break
                delta, is_first = message_payload(chunk, streaming=True), False
            if not finished:
                yield stream_event({}, identifier=identifier, model=payload.model, created=created, reason="stop")
            if payload.include_usage and usage is not None:
                yield stream_event({}, identifier=identifier, model=payload.model, created=created, usage=usage)
            yield "data: [DONE]\n\n"
            outcome[0] = True
        except (asyncio.CancelledError, GeneratorExit):
            outcome[0] = None
            raise
        except Exception:
            outcome[0] = False
            yield f"data: {compact_json(error_payload('上游流在输出后中断', 'upstream_stream_error'))}\n\n"
            yield "data: [DONE]\n\n"
