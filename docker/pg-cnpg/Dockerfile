ARG PG_MAJOR
ARG FROM_TAG
ARG SCHEMA
ARG TARGETARCH

FROM modelzai/pgvecto-rs-binary:${FROM_TAG}-${TARGETARCH}-${SCHEMA} as binary

FROM rust:1.78-bookworm as builder
ARG TRUNK_VER=0.12.25
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL sparse 
RUN cargo install --version $TRUNK_VER pg-trunk

# FROM modelzai/pg-slim:${PG_MAJOR}
FROM xieydd/pg-slim:${PG_MAJOR}
ARG PG_MAJOR

USER root

COPY --from=binary /pgvecto-rs-binary-release.deb /tmp/vectors.deb
RUN apt install -y /tmp/vectors.deb && rm -f /tmp/vectors.deb

# PGDATA is set in pg-slim and used by dependents on this image.
RUN if [ -z "${PGDATA}" ]; then echo "PGDATA is not set"; exit 1; fi

# Install trunk
COPY --from=builder /usr/local/cargo/bin/trunk /usr/bin/trunk
COPY ./requirements.txt .

# Install barman-cloud
RUN set -xe; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		python3-pip \
		python3-psycopg2 \
		python3-setuptools \
	; \
	pip3 install --upgrade pip; \
# TODO: Remove --no-deps once https://github.com/pypa/pip/issues/9644 is solved
	pip3 install --no-deps -r requirements.txt; \
  apt-get autoremove -y; \
  apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

RUN chown -R postgres:postgres /usr/lib/postgresql/${PG_MAJOR} && \
    chmod -R 0700 /usr/lib/postgresql/${PG_MAJOR}
RUN chown postgres /usr/share/postgresql/${PG_MAJOR}/extension

RUN apt-get update && apt-get install -y \
    jq \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install extension dependencies
RUN apt-get update && apt-get install -y \
    libmysqlclient-dev \
    libtcl8.6 \
    libgeos-dev \
    libproj-dev \
    libjson-c-dev \
    libprotobuf-c-dev \
    libxml2-dev \
    libboost-serialization1.74-dev \
    libhiredis-dev \
    libsybdb5 \
    libpython3.10-dev \
    r-base-core \
    openssl \
    liblz4-1 \
    libpcre2-8-0 \
    libuuid1 \
    libgroonga0 \
    libopenblas0-pthread \
    libcurl4 \
    libjson-c5 \
    libsodium23 \
    libgcc-s1 \
    libselinux1 \
    librdkafka1 \
    libgdal30 \
    libcrypt1 \
    liburiparser1 \
    libfreetype6 \
    libzstd1 \
    zlib1g \
    libperl5.34 \
    libgomp1 \
    libssl3 \
    libsfcgal1 \
    openjdk-11-jdk \
    libaio1 \
    libbson-dev \
    libgsl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so /usr/lib/x86_64-linux-gnu/libjvm.so
RUN wget https://download.oracle.com/otn_software/linux/instantclient/1920000/instantclient-basiclite-linux.x64-19.20.0.0.0dbru.zip && \
    unzip instantclient-basiclite-linux.x64-19.20.0.0.0dbru.zip && \
    cp instantclient_19_20/libclntsh.so.19.1 /usr/lib/x86_64-linux-gnu/ && \
    cp instantclient_19_20/libnnz19.so /usr/lib/x86_64-linux-gnu/ && \
    cp instantclient_19_20/libclntshcore.so.19.1 /usr/lib/x86_64-linux-gnu/ && \
    rm -rf instantclient_19_20 && \
    rm instantclient-basiclite-linux.x64-19.20.0.0.0dbru.zip

# Install zhparser dependency
RUN  wget http://www.xunsearch.com/scws/down/scws-1.2.3.tar.bz2 && \
     tar xvf scws-1.2.3.tar.bz2 && \
     cd scws-1.2.3 && \
     ./configure && \
     make install && \
     cd .. && \
     rm -rf scws-1.2.3.tar.bz2 scws-1.2.3 && \
     ln -s /usr/local/lib/libscws.so /usr/lib/x86_64-linux-gnu/libscws.so

# Install duckdb libs
RUN wget https://github.com/duckdb/duckdb/releases/download/v0.8.1/libduckdb-linux-amd64.zip && \
    unzip libduckdb-linux-amd64.zip && \
    cp libduckdb.so /usr/lib/x86_64-linux-gnu/ && \
    rm -rf libduckdb-linux-amd64.zip libduckdb.so

# Test trunk
COPY trunk-install.sh /usr/local/bin/

# Install pg_stat_statements
RUN trunk install pg_stat_statements

# Install auto_explain
RUN trunk install auto_explain

# Install plpython3u
RUN trunk install plpython3u

# cache pg_stat_statements and auto_explain and pg_stat_kcache to temp directory
RUN set -eux; \
	  mkdir /tmp/pg_pkglibdir; \
	  mkdir /tmp/pg_sharedir; \
      cp -r $(pg_config --pkglibdir)/* /tmp/pg_pkglibdir; \
      cp -r $(pg_config --sharedir)/* /tmp/pg_sharedir

# Clone and build AWS SDK for C++
RUN git clone https://github.com/aws/aws-sdk-cpp.git && \
    cd aws-sdk-cpp && \
    git checkout 1.9.263 && \
    git submodule update --init --recursive && \
    mkdir build && cd build && \
    cmake -DBUILD_ONLY="s3;core;config;sts;cognito-identity;transfer;identity-management" -DAUTORUN_UNIT_TESTS=OFF -DCMAKE_CXX_FLAGS=-Wno-error=deprecated-declarations .. && \ 
    make -j$(nproc) && \
    make install && \
    cd ../../../ && rm -rf aws-sdk-cpp

# Clone and build Apache Arrow
RUN git clone https://github.com/apache/arrow.git && \
    cd arrow && \
    git checkout apache-arrow-7.0.1 && \
    cd cpp && \
    mkdir build && cd build && \
    cmake -DARROW_PARQUET=ON -DARROW_S3=ON -DARROW_WITH_SNAPPY=ON .. && \
    make -j$(nproc) && \
    make install && \
    cd ../../../ && rm -rf arrow

# Clone and build pgaudit
RUN git clone https://github.com/pgaudit/pgaudit.git && \
    cd pgaudit && \
    git checkout REL_${PG_MAJOR}_STABLE && \
    make install USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config && \
    cd ../ && rm -rf pgaudit

# Clone and build pg_failover_slots
RUN git clone https://github.com/EnterpriseDB/pg_failover_slots.git && \
    cd pg_failover_slots && \
    make install PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config && \
    cd ../ && rm -rf pg_failover_slots 

# cache all extensions
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

COPY ./trunk-install.sh /usr/local/bin/
# Change the uid of postgres to 26
RUN usermod -u 26 postgres
RUN chown -R postgres:postgres /usr/lib/postgresql/${PG_MAJOR}
RUN cp /usr/share/postgresql/${PG_MAJOR}/extension/* /usr/lib/postgresql/${PG_MAJOR}/share/extension/
USER 26
ENV PATH $PATH:/usr/lib/postgresql/${PG_MAJOR}/bin