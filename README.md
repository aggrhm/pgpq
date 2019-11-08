# Deploy

1. Commit and push to github

2. Push release

```
$ make build dist tag=v1.1.6
```

# Migrate

```
$ ./pgpq -storeurl=$PGPQ_STORE_URL -migrate
```

# Run

```
$ ./pgpq -storeurl=$PGPQ_STORE_URL
```
