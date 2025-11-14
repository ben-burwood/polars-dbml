# Polars DBML

Convert a Polars Schema to DBML.

## Input

```python
USERS_SCHEMA = {
    "id": pl.Int64,
    "username": pl.String,
    "role": pl.String,
    "created_at": pl.Datetime
}
```

## Output

```dbml
Table users {
  id integer
  username varchar
  role varchar
  created_at timestamp
}
```
