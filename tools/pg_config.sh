#!/usr/bin/env bash
set -e

source=$(cat -)

if [ -z "$source" ]; then
	echo "pg_config: could't find configuration file"
	exit 1
fi

for arg in "$@"; do
	if [ "$arg" = "--help" ] || [ "$arg" = "-?" ]; then
		cat <<EOF

pg_config provides information about the installed version of PostgreSQL.

Usage:
  pg_config [OPTION]...

Options:
  --bindir              show location of user executables
  --docdir              show location of documentation files
  --htmldir             show location of HTML documentation files
  --includedir          show location of C header files of the client
                        interfaces
  --pkgincludedir       show location of other C header files
  --includedir-server   show location of C header files for the server
  --libdir              show location of object code libraries
  --pkglibdir           show location of dynamically loadable modules
  --localedir           show location of locale support files
  --mandir              show location of manual pages
  --sharedir            show location of architecture-independent support files
  --sysconfdir          show location of system-wide configuration files
  --pgxs                show location of extension makefile
  --configure           show options given to "configure" script when
                        PostgreSQL was built
  --cc                  show CC value used when PostgreSQL was built
  --cppflags            show CPPFLAGS value used when PostgreSQL was built
  --cflags              show CFLAGS value used when PostgreSQL was built
  --cflags_sl           show CFLAGS_SL value used when PostgreSQL was built
  --ldflags             show LDFLAGS value used when PostgreSQL was built
  --ldflags_ex          show LDFLAGS_EX value used when PostgreSQL was built
  --ldflags_sl          show LDFLAGS_SL value used when PostgreSQL was built
  --libs                show LIBS value used when PostgreSQL was built
  --version             show the PostgreSQL version
  -?, --help            show this help, then exit

With no arguments, all known items are shown.

Report bugs to <pgsql-bugs@lists.postgresql.org>.
PostgreSQL home page: <https://www.postgresql.org/>
EOF
		exit 0
	fi
done

if [ $# -eq 0 ]; then
  echo "$source"
	exit 0
fi

for arg in "$@"; do
	res=""

	if [[ "$arg" == --* ]]; then
		var=$(echo "$arg" | cut -c 3- | tr '[:lower:]' '[:upper:]')
		res=$(printf "%s" "$source" | grep -E "^$var = " - | cut -d "=" -f "2-")
	fi

	if [ -z "$res" ]; then
		echo "pg_config: invalid argument: $arg"
		echo "Try "pg_config --help" for more information."
		exit 1
	fi

    echo $res
done
