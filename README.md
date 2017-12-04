# sqlmat
Simple sql build for asyncpg, map python statement to sql.

## examples
```
# select all rows
await sqlmat.table('user').filter(name='tom').select()
```

```
# update
await sqlmat.table('user').filter(gendor='male').update(gendor='female')
```

```
# get one row
await sqlmat.table('user').filter(city='beijing').get_one()
```

```
# set default pool
pool = ...   # get asyncpg pool
sqlmat.set_default_pool(pool)
```

```
# transaction using connection
pool = ...
async with pool.acquire() as conn:
    async with conn.transaction():
        await sqlmat.table(
            'user').using(
                conn).filter(grade=3).update(
                    grade=sqlmat.F('grade') + 3)
```

