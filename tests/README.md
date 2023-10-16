## Tests for pgvecto.rs

We use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to test the SQL queries.

To run all tests, use the following command:
```shell
sqllogictest './tests/**/*.slt'
```

Each time you modify the source code, you can run the following command to clean up the test data and reload the extension:
```shell
psql -f ./tests/init.sql
```