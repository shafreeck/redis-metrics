# Export prometheus metrics of redis

# Features

* Dynamicly discover hosts from the DNS 
* Dynamicly discover all slaves of the master
* Export rich metrics
* Zero downtime binary upgrading

# Usage

```
go get github.com/shafreeck/redis-metrics
```

Edit redis-metrics.toml

```
auth=""
master=["your", "redis", "masters"]
```

# Upgrade

```
mv redis-metrics redis-metrics.old
cp /new/redis-metrics ./redis-metrics
kill -HUP $(redis-metrics.pid)
```
