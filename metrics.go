package main

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type KeyHandler func(rds *Redis, val string) prometheus.Metric

type RedisCollector struct {
	conf     *Conf
	handlers map[string]KeyHandler
}

func (rc *RedisCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(rc, ch)
}

func (rc *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	RunOnRedis(ParseRedisTopology(rc.conf.Masters, rc.conf.Auth), func(rds *Redis) {
		text := rds.Client.Info().String()
		lines := strings.Split(text, "\r\n")
		for _, line := range lines {
			fields := strings.Split(line, ":")
			if len(fields) < 2 {
				continue
			}

			key, val := fields[0], fields[1]
			if strings.HasPrefix(key, "db") {
				collectDB(rds, ch, key, val)
				continue
			}

			h, ok := rc.handlers[key]
			if !ok {
				continue
			}
			ch <- h(rds, val)
		}

		collectCommandStats(rds, ch)
	})
}

// Metric generate a key handler that parse a single metric
// labelPairs is the labelName1, labelValue1, labelName2, labelValue2 ... pairs
func Metric(name string, desc string, labelPairs ...string) KeyHandler {
	return func(rds *Redis, val string) prometheus.Metric {
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			log.Printf("%v parse %v to float64 failed, %v\n", name, val, err)
		}
		return MetricWithValue(name, desc, labelPairs...)(rds, v)
	}
}

// MetricWithValue return a function that accept a float64 value
func MetricWithValue(name string, desc string, labelPairs ...string) func(rds *Redis, val float64) prometheus.Metric {
	var labelNames, labelValues []string
	for i := 0; i < len(labelPairs)-1; i += 2 {
		labelNames = append(labelNames, labelPairs[i])
		labelValues = append(labelValues, labelPairs[i+1])
	}

	return func(rds *Redis, val float64) prometheus.Metric {
		desc := prometheus.NewDesc(
			name,
			desc,
			append([]string{"host", "role"}, labelNames...), nil)
		return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, val, append([]string{rds.Address, rds.Role}, labelValues...)...)
	}
}

func NewRedisCollector(c *Conf) *RedisCollector {
	handlers := map[string]KeyHandler{
		"redis_version": func(rds *Redis, val string) prometheus.Metric {
			return MetricWithValue("redis_version", "version of the redis instance", "version", val)(rds, float64(1))
		},

		"uptime_in_seconds":          Metric("redis_up_seconds_total", "seconds since startup of the redis"),
		"used_memory_rss":            Metric("redis_memory_usage_bytes", "redis memory usage", "type", "rss"),
		"used_memory":                Metric("redis_memory_usage_bytes", "redis memory usage", "type", "zmalloc"),
		"used_memory_peak":           Metric("redis_memory_usage_bytes", "redis memory usage", "type", "peak"),
		"used_memory_lua":            Metric("redis_memory_usage_bytes", "redis memory usage", "type", "lua"),
		"used_memory_scripts":        Metric("redis_memory_usage_bytes", "redis memory usage", "type", "scripts"),
		"maxmemory":                  Metric("redis_max_memory_bytes", "redis max memory configured, 0 means unlimited"),
		"total_system_memory":        Metric("redis_memory_system_total_bytes", "total memory of the host"),
		"connected_clients":          Metric("redis_clients_connected_number", "number of redis clients"),
		"total_net_input_bytes":      Metric("redis_net_bytes_total", "network flowed bytes", "direction", "input"),
		"total_net_output_bytes":     Metric("redis_net_bytes_total", "network flowed bytes", "direction", "output"),
		"used_cpu_sys":               Metric("redis_cpu_usage_seconds_total", "total seconds of cpu time", "type", "sys"),
		"used_cpu_user":              Metric("redis_cpu_usage_seconds_total", "total seconds of cpu time", "type", "user"),
		"total_commands_processed":   Metric("redis_commands_processed_total", "total number of commands has been processed"),
		"blocked_clients":            Metric("redis_clients_blocked_number", "number of blocked clients"),
		"loading":                    Metric("redis_current_status", "current status of redis", "status", "loading"),
		"rdb_bgsave_in_progres":      Metric("redis_current_status", "current status of redis", "status", "rdb_bgsave_in_progres"),
		"aof_enabled":                Metric("redis_current_status", "current status of redis", "status", "aof_enabled"),
		"aof_rewrite_in_progress":    Metric("redis_current_status", "current status of redis", "status", "aof_rewrite_in_progress"),
		"aof_rewrite_scheduled":      Metric("redis_current_status", "current status of redis", "status", "aof_rewrite_scheduled"),
		"cluster_enabled":            Metric("redis_current_status", "current status of redis", "status", "cluster_enabled"),
		"total_connections_received": Metric("redis_connections_total", "number of connections processed", "type", "received"),
		"rejected_connections":       Metric("redis_connections_total", "number of connections processed", "type", "rejected"),
		"expired_keys":               Metric("redis_expired_keys_total", "number of keys that has expired"),
		"keyspace_hits":              Metric("redis_keyspace_access", "hits or misses of key requests", "type", "hits"),
		"keyspace_misses":            Metric("redis_keyspace_access", "hits or misses of key requests", "type", "misses"),
		"latest_fork_usec":           Metric("redis_latest_fork_duration_microseconds", "cost of the lastest fork"),
	}

	return &RedisCollector{conf: c, handlers: handlers}
}

func collectCommandStats(rds *Redis, ch chan<- prometheus.Metric) {
	text := rds.Client.Info("commandstats").String()
	lines := strings.Split(text, "\r\n")

	prefix := "cmdstat_"
	for _, line := range lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		cmd, calls, usec := parseCommandStats(rds, line)
		ch <- MetricWithValue("redis_commands_calls_total", "total number of commands has been called", "cmd", cmd)(rds, float64(calls))
		ch <- MetricWithValue("redis_commands_duration_seconds_total", "total seconds of all commands duration", "cmd", cmd)(rds, float64(usec)/1000000)
	}
}

func parseCommandStats(rds *Redis, line string) (string, int64, int64) {
	// cmdstat_info:calls=74,usec=3488,usec_per_call=47.14
	re := regexp.MustCompile("cmdstat_(.*):calls=([0-9]+),usec=([0-9]+),usec_per_call=(.*)")
	matches := re.FindStringSubmatch(line)
	if len(matches) < 5 {
		return "", 0, 0
	}
	cmd := matches[1]
	calls, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		log.Println("parse commandstats(calls) failed", err)
	}

	usec, err := strconv.ParseInt(matches[3], 10, 64)
	if err != nil {
		log.Println("parse commandstats(usec) failed", err)
	}
	return cmd, calls, usec
}

// collectDB collects keys, expires, avg_ttl of a db
func collectDB(rds *Redis, ch chan<- prometheus.Metric, db, val string) {
	// db0:keys=3803437,expires=3216105,avg_ttl=2565570118
	re := regexp.MustCompile("keys=([0-9]+),expires=([0-9]+),avg_ttl=([0-9]+)")
	matches := re.FindStringSubmatch(val)
	if len(matches) < 4 {
		return
	}

	keys, expires, ttl := matches[1], matches[2], matches[3]
	ch <- Metric("redis_keyspace_keys_total", "total number of keys", "db", db)(rds, keys)
	ch <- Metric("redis_keyspace_expires_total", "total number of keys to expire", "db", db)(rds, expires)
	v, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil {
		log.Println("parse keyspace(avg_ttl) failed", err)
	}
	ch <- MetricWithValue("redis_keyspace_avg_ttl_seconds", "average ttl of keys", "db", db)(rds, float64(v)/1000)
}
