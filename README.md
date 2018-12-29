# Export prometheus metrics of redis

# Features

* Dynamicly discover hosts from the DNS 
* Dynamicly discover all slaves of the master
* Export rich metrics

# Usage

```
go get github.com/shafreeck/redis-metrics
```

Edit redis-metrics.toml

```
auth=""
master=["your", "redis", "masters"]
```
