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

ARG BUILD_REVISION=unknown
ARG RELEASE_ID=unknown
LABEL org.opencontainers.image.revision=$BUILD_REVISION
LABEL org.opencontainers.image.version=$RELEASE_ID
# Written after COPY: a host checkout's manifest cannot replace the image identity.
RUN BUILD_REVISION="$BUILD_REVISION" RELEASE_ID="$RELEASE_ID" python -c 'import json, os; from pathlib import Path; Path("build-info.json").write_text(json.dumps({"revision": os.environ["BUILD_REVISION"], "release_id": os.environ["RELEASE_ID"]}))'
# Exercise the installed wheel and public dispatcher in the FINAL image stage.
RUN python -m analytics.release_check --require-rust --allow-unversioned

ENV HOST=0.0.0.0
ENV PORT=5000

EXPOSE 5000

CMD ["python", "-m", "production"]
