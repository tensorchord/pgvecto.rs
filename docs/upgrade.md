# Upgrade

## `The extension is upgraded. However, the index files is outdated.`

You may see this error if you upgrade the extension. On this condition, you should follow these steps:

* Delete the old index folder.

You can delete the folder with this command:

```shell
rm -rf $(psql -U postgres -tAqX -c $'SELECT CONCAT(CURRENT_SETTING(\'data_directory\'), \'/pg_vectors\');')
```

If you are using Docker, you can just delete `pg_vectors` folder under the volume directory too.

You need to restart PostgreSQL.

* Reindex.

You can list all indexes that needed to be reindexed with this command:

```sql
SELECT
    I.oid AS indexrelid,
    I.relname AS indexname
FROM pg_index X
     JOIN pg_class I ON I.oid = X.indexrelid
     JOIN pg_am A ON A.oid = I.relam
WHERE A.amname = 'vectors';
```

If you get the result like this:

```
 indexrelid | indexname  
------------+------------
      17988 | t_val_idx
      17989 | t_val_idx1
      17990 | t_val_idx2
      17991 | t_val_idx3
```

You will reindex them with this SQL:

```sql
REINDEX INDEX t_val_idx;
REINDEX INDEX t_val_idx1;
REINDEX INDEX t_val_idx2;
REINDEX INDEX t_val_idx3;
```
