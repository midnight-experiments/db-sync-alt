FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
# PostgreSQL 17 client tools from the PostgreSQL apt repository (apt.postgresql.org). Its signing key
# (fingerprint B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8) is kept in deploy/ and pinned by SHA-256 here.
COPY deploy/postgresql-apt.asc /usr/share/keyrings/postgresql-apt.asc
RUN echo '0144068502a1eddd2a0280ede10ef607d1ec592ce819940991203941564e8e76  /usr/share/keyrings/postgresql-apt.asc' | sha256sum --check --strict - \
    && echo 'deb [signed-by=/usr/share/keyrings/postgresql-apt.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main' > /etc/apt/sources.list.d/postgresql.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-client-17 ca-certificates && rm -rf /var/lib/apt/lists/* \
    && pg_dump --version | grep -q '(PostgreSQL) 17\.' && pg_restore --version | grep -q '(PostgreSQL) 17\.'
WORKDIR /opt/db-sync-alt
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY pyproject.toml README.md CHANGELOG.md LICENSE NOTICE THIRD_PARTY_NOTICES.md ./
COPY src ./src
COPY profiles ./profiles
COPY reference ./reference
RUN pip install --no-cache-dir --no-deps --no-build-isolation . && rm -rf build src/*.egg-info
COPY tests ./tests
COPY tools ./tools
COPY deploy ./deploy
COPY docs/operations.md ./docs/operations.md
COPY docs/licenses ./docs/licenses
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 65532:65532
CMD ["db-sync-alt", "--help"]
