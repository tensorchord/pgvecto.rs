FROM ubuntu:22.04

# 15, 16, 17
ARG PG_MAJOR
# 15.8-1.pgdg120+1 16.4-1.pgdg120+2 17.0-1.pgdg120+1
# ARG PG_VERSION 
ARG DEBIAN_FRONTEND=noninteractive
ARG ALTDIR=/var/lib/postgresql/data/tensorchord
ENV TZ=Etc/UTC
ENV PGDATA /var/lib/postgresql/data

# Get latest package updates
RUN set -eux; \
      apt-get update; \
      apt-get upgrade -y 

# explicitly set user/group IDs
RUN set -eux; \
	groupadd -r postgres --gid=999; \
# https://salsa.debian.org/postgresql/postgresql-common/blob/997d842ee744687d99a2b2d95c1083a2615c79e8/debian/postgresql-common.postinst#L32-35
	useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres; \
# also create the postgres user's home directory with appropriate permissions
# see https://github.com/docker-library/postgres/issues/274
	install --verbose --directory --owner postgres --group postgres --mode 1777 /var/lib/postgresql

RUN set -eux; \
      apt-get update; \
      apt-get install -y --no-install-recommends \
      locales curl ca-certificates gnupg lsb-release lbzip2 git cmake \
    ; \
    rm -rf /var/lib/apt/lists/*; \
          localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.utf8

RUN mkdir /docker-entrypoint-initdb.d

RUN git clone https://github.com/postgres/postgres.git && \
    cd postgres && \
    PG_RELEASE=$(git tag | grep '^REL_'${PG_MAJOR}'_' | grep -vE 'RC|BETA' | sort -V | tail -1 | sed 's/REL_//') && \
    echo $PG_RELEASE && \
    git checkout REL_$PG_RELEASE

RUN set -eux; \
      apt-get update && apt-get install -y \
          libreadline-dev \
          zlib1g-dev \
          libpq-dev \
          build-essential \
          python3-dev \
          tcl-dev \
          libxslt1-dev \
          libperl-dev \
          libpam0g-dev \
          libreadline-dev \
          libssl-dev \
          xz-utils \
          libnss-wrapper \
          llvm \
          clang \
          icu-devtools \
          pkg-config \
          libgss-dev \
          libkrb5-dev \
          uuid-dev \
          gettext \
          liblz4-dev \
          libsystemd-dev \
          libselinux1-dev \
          libzstd-dev \
            vim \
          flex \
          bison; \
      apt-get autoremove -y; \
      apt-get clean -y; \
      rm -rf /var/lib/apt/lists/*

WORKDIR postgres

ENV CFLAGS "-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -fno-omit-frame-pointer"
ENV LDFLAGS "-Wl,-z,relro -Wl,-z,now"
RUN ./configure --prefix=/usr/lib/postgresql/${PG_MAJOR} \
        --datarootdir=${ALTDIR} \
        --libdir=${ALTDIR}/${PG_MAJOR}/lib \
        --with-perl \
        --with-python \
        --with-tcl \
        --with-pam \
        --with-libxml \
        --with-libxslt \
        --with-openssl \
        --enable-nls \
        --enable-thread-safety \
        --enable-debug \
        --disable-rpath \
        --with-uuid=e2fs \
        --with-gnu-ld \
        --with-gssapi \
        --with-pgport=5432 \
        --with-system-tzdata=/usr/share/zoneinfo \
        --with-icu \
        --with-llvm \
        --with-lz4 \
        --with-zstd \
        --with-systemd \
        --with-selinux

RUN make -j$(nproc)
RUN make install

WORKDIR /
RUN rm -rf /postgres

RUN mkdir -p /var/run/postgresql && chmod 775 /var/run/postgresql
RUN mkdir -p /usr/share/postgresql/${PG_MAJOR}/extension && chmod 775 /usr/share/postgresql/${PG_MAJOR}/extension

COPY --from=tianon/gosu /gosu /usr/local/bin/

# make the sample config easier to munge (and "correct by default")
RUN set -eux; \
    echo "unix_socket_directories = '/var/run/postgresql'" >> ${ALTDIR}/postgresql.conf.sample; \
	sed -ri "s!^#?(listen_addresses)\s*=\s*\S+.*!\1 = '*'!" ${ALTDIR}/postgresql.conf.sample; \
	grep -F "listen_addresses = '*'" ${ALTDIR}/postgresql.conf.sample

RUN install --verbose --directory --owner postgres --group postgres --mode 3777 /var/run/postgresql

# this 1777 will be replaced by 0700 at runtime (allows semi-arbitrary "--user" values)
RUN install --verbose --directory --owner postgres --group postgres --mode 1777 ${PGDATA}

ENV PATH $PATH:/usr/lib/postgresql/$PG_MAJOR/bin:/usr/local/bin
COPY docker-entrypoint.sh docker-ensure-initdb.sh /usr/local/bin/
RUN ln -sT docker-ensure-initdb.sh /usr/local/bin/docker-enforce-initdb.sh
ENTRYPOINT ["docker-entrypoint.sh"]

# Remove pre-installed pg_config
RUN rm /usr/bin/pg_config

STOPSIGNAL SIGINT
EXPOSE 5432
CMD ["postgres"]