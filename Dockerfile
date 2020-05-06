FROM postgres:11 AS builder
WORKDIR /mnt

RUN apt-get update && \
    apt-get install -y postgresql-11-wal2json postgresql-server-dev-11 make gcc ruby ruby-dev sudo && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p src test
COPY ["Makefile", "./"]
COPY ["src", "./src/"]
COPY ["test", "./test/"]

RUN sudo -u postgres /mnt/test/setup_postgres.sh /usr/lib/postgresql/11/bin /var/lib/postgresql/data && \
    env PGHOST=localhost PGUSER=postgres PGDATABASE=test make

FROM postgres:11
WORKDIR /mnt
COPY --from=builder /mnt/src/pg_logical_stream /usr/bin/
CMD pg_logical_stream

