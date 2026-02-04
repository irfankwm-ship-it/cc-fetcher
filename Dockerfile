# syntax=docker/dockerfile:1.4
# Secure multi-stage build for cc-fetcher

# =============================================================================
# Stage 1: Build dependencies
# =============================================================================
FROM python:3.12-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VERSION=1.8.2
RUN python -m venv $POETRY_HOME && \
    $POETRY_HOME/bin/pip install poetry==$POETRY_VERSION

# Set up build directory
WORKDIR /build

# Copy dependency files first (better caching)
COPY pyproject.toml poetry.lock* ./

# Export dependencies to requirements.txt (no dev deps)
RUN $POETRY_HOME/bin/poetry export -f requirements.txt --without dev --output requirements.txt

# Install dependencies into a virtual environment
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy source code and install the package
COPY src/ ./src/
COPY README.md ./
RUN /opt/venv/bin/pip install --no-cache-dir .

# Install Playwright browsers (chromium only for smaller image)
RUN /opt/venv/bin/playwright install chromium --with-deps

# =============================================================================
# Stage 2: Runtime image (minimal, hardened)
# =============================================================================
FROM python:3.12-slim-bookworm AS runtime

# Security: Create non-root user with no shell
RUN groupadd --gid 1000 fetcher && \
    useradd --uid 1000 --gid fetcher --shell /usr/sbin/nologin --create-home fetcher

# Install only runtime dependencies for Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    # CA certificates for HTTPS
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    # Remove unnecessary files
    && rm -rf /tmp/* /var/tmp/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy Playwright browsers from builder
COPY --from=builder /root/.cache/ms-playwright /home/fetcher/.cache/ms-playwright
RUN chown -R fetcher:fetcher /home/fetcher/.cache

# Copy config files
WORKDIR /app
COPY --chown=fetcher:fetcher config/ ./config/

# Set environment
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # Playwright settings
    PLAYWRIGHT_BROWSERS_PATH=/home/fetcher/.cache/ms-playwright

# Create output directory (will be mounted as volume)
RUN mkdir -p /data/raw && chown -R fetcher:fetcher /data

# Security: Switch to non-root user
USER fetcher

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import fetcher; print('healthy')" || exit 1

# Default command
ENTRYPOINT ["fetcher"]
CMD ["run", "--env", "prod", "--output-dir", "/data/raw"]
