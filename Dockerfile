FROM rust:1.83-slim-bookworm AS analytics-builder
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN python3 -m venv /build/venv && /build/venv/bin/pip install --no-cache-dir 'maturin>=1.7,<2'
COPY rust/ ./rust/
RUN /build/venv/bin/maturin build --release --locked --manifest-path rust/Cargo.toml --interpreter /build/venv/bin/python --out /wheels

FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=analytics-builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

COPY . .

ENV HOST=0.0.0.0
ENV PORT=5000

EXPOSE 5000

CMD ["python", "app.py"]
