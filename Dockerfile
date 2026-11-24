# syntax=docker/dockerfile:1
# 版本标签固定；升级时同时检查 p115client 的兼容性。
ARG PYTHON_IMAGE=python:3.12.12-slim-bookworm

# ---- 构建阶段：按 uv.lock 安装运行依赖 ----
FROM ${PYTHON_IMAGE} AS builder
COPY --from=ghcr.io/astral-sh/uv:0.13.4 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# ---- 运行阶段：只带虚拟环境和入口脚本，不含任何密钥 ----
FROM ${PYTHON_IMAGE}
COPY --from=builder /opt/venv /opt/venv
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
# 防止 Windows 检出的 CRLF 换行破坏 shebang。
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh \
    && chmod 0755 /usr/local/bin/entrypoint.sh \
    && mkdir -p /config /data

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BRIDGE_CONFIG=/config/config.yml \
    PUID=1024 \
    PGID=100 \
    UMASK=000

VOLUME ["/config", "/data"]
EXPOSE 8080

# /healthz 无需认证，只返回 ok；端口取自配置文件的 webui.port（默认 8080）。
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD ["python", "-m", "bridge.healthcheck"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "bridge"]
