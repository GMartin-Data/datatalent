FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# 1. Dépendances Python (cache Docker: change rarement)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# 2. Config dbt
COPY dbt_project.yml packages.yml package-lock.yml ./
COPY profiles.yml.example profiles.yml
ENV DBT_PROFILES_DIR=/app

# 3. dbt deps (installe dbt_utils depuis packages.yml)
RUN uv run dbt deps

# 4. Code source dbt
COPY models models/
COPY macros macros/
COPY tests tests/

ENTRYPOINT ["uv", "run", "dbt"]