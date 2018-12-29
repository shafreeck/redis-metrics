package main

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

/*
*  All Redis Metrics
*  redis_version
*  redis_up_seconds_total
*  redis_clients_connected_number
*  redis_clients_blocked_number
*  redis_client_longest_output_list_size
*  redis_client_biggest_input_buf_size
*  redis_memory_usage_bytes [rss, peak, lua]
*  redis_memory_system_total_bytes
*  redis_max_memory_bytes
*  redis_memory_fragmentation_ratio
*  redis_active_defrag_running
*  redis_rdb_loading
*  redis_rdb_changes_since_last_save
*  redis_rdb_bgsave_in_progress
*  redis_aof_enabled
*  redis_aof_rewrite_in_progress
*  redis_connections_total [received, rejected]
*  redis_commands_processed_total
*  redis_commands_calls_total
*  redis_net_bytes_total
*  redis_repl_backlog_size
*  redis_cpu_usage_seconds_total [sys, user]
*  redis_keys_number
*  redis_expires_number
*
 */

type RedisCollector struct {
	conf *Conf
}

func NewRedisCollector(c *Conf) *RedisCollector {
	return &RedisCollector{conf: c}
}

func (rc *RedisCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(rc, ch)
}

func (rc *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	RunOnRedis(ParseRedisTopology(rc.conf.Masters, rc.conf.Auth), func(rds *Redis) {
		text := rds.Client.Info().String()
		lines := strings.Split(text, "\r\n")
		for _, line := range lines {
			switch {
			case strings.HasPrefix(line, "redis_version:"):
				ch <- redisVersion(rds, line)
			case strings.HasPrefix(line, "uptime_in_seconds:"):
				ch <- redisUptime(rds, line)
			case strings.HasPrefix(line, "used_memory_rss:"):
				ch <- redisRSSMemoryUsage(rds, line)
			case strings.HasPrefix(line, "maxmemory:"):
				ch <- redisMaxMemory(rds, line)
			case strings.HasPrefix(line, "total_system_memory:"):
				ch <- redisSystemMemory(rds, line)
			case strings.HasPrefix(line, "connected_clients:"):
				ch <- redisClientsConnected(rds, line)
			case strings.HasPrefix(line, "total_net_input_bytes:"):
				ch <- redisNetInputBytes(rds, line)
			case strings.HasPrefix(line, "total_net_output_bytes:"):
				ch <- redisNetOutputBytes(rds, line)
			case strings.HasPrefix(line, "used_cpu_sys:"):
				ch <- redisCPUSys(rds, line)
			case strings.HasPrefix(line, "used_cpu_user:"):
				ch <- redisCPUUser(rds, line)
			case strings.HasPrefix(line, "total_commands_processed:"):
				ch <- redisCommandsProcessedTotal(rds, line)
			}
		}

		collectCommandStats(rds, ch)
	})
}

func redisVersion(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_version",
		"version of the redis instance",
		[]string{"host", "role", "version"}, nil)
	version := strings.TrimPrefix(line, "redis_version:")
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(1), rds.Address, rds.Role, version)
}

func redisUptime(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_up_seconds_total",
		"seconds since startup of the redis",
		[]string{"host", "role"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "uptime_in_seconds:"), 10, 64)
	if err != nil {
		log.Println("parse uptime failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(val), rds.Address, rds.Role)
}

func redisRSSMemoryUsage(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_memory_usage_bytes",
		"redis memory usage",
		[]string{"host", "role", "type"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "used_memory_rss:"), 10, 64)
	if err != nil {
		log.Println("parse used_memory_rss failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(val), rds.Address, rds.Role, "rss")
}

func redisMaxMemory(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_max_memory_bytes",
		"redis max memory configured, 0 means unlimited",
		[]string{"host", "role"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "maxmemory:"), 10, 64)
	if err != nil {
		log.Println("parse maxmemory failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(val), rds.Address, rds.Role)
}

func redisSystemMemory(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_memory_system_total_bytes",
		"total memory of the host",
		[]string{"host", "role"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "total_system_memory:"), 10, 64)
	if err != nil {
		log.Println("parse total_system_memory failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(val), rds.Address, rds.Role)
}

func redisClientsConnected(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_clients_connected_number",
		"number of redis clients",
		[]string{"host", "role"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "connected_clients:"), 10, 64)
	if err != nil {
		log.Println("parse connected_clients failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(val), rds.Address, rds.Role)
}

func redisNetInputBytes(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_net_bytes_total",
		"network flowed bytes",
		[]string{"host", "role", "direction"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "total_net_input_bytes:"), 10, 64)
	if err != nil {
		log.Println("parse total_net_input_bytes failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(val), rds.Address, rds.Role, "input")
}

func redisNetOutputBytes(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_net_bytes_total",
		"network flowed bytes",
		[]string{"host", "role", "direction"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "total_net_output_bytes:"), 10, 64)
	if err != nil {
		log.Println("parse total_net_output_bytes failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(val), rds.Address, rds.Role, "output")
}

func redisCPUSys(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_cpu_usage_seconds_total",
		"total seconds of cpu time",
		[]string{"host", "role", "type"}, nil)
	val, err := strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_sys:"), 64)
	if err != nil {
		log.Println("parse used_cpu_sys failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, val, rds.Address, rds.Role, "sys")
}

func redisCPUUser(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_cpu_usage_seconds_total",
		"total seconds of cpu time",
		[]string{"host", "role", "type"}, nil)
	val, err := strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_user:"), 64)
	if err != nil {
		log.Println("parse used_cpu_user failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, val, rds.Address, rds.Role, "user")
}

func redisCommandsProcessedTotal(rds *Redis, line string) prometheus.Metric {
	desc := prometheus.NewDesc(
		"redis_commands_processed_total",
		"total number of commands has been processed",
		[]string{"host", "role", "cmd"}, nil)
	val, err := strconv.ParseInt(strings.TrimPrefix(line, "total_commands_processed:"), 10, 64)
	if err != nil {
		log.Println("parse total_commands_processed failed", err)
	}
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(val), rds.Address, rds.Role, "total")
}
func collectCommandStats(rds *Redis, ch chan<- prometheus.Metric) {
	callsDesc := prometheus.NewDesc(
		"redis_commands_calls_total",
		"total number of commands has been called",
		[]string{"host", "role", "cmd"}, nil)
	durationDesc := prometheus.NewDesc(
		"redis_commands_duration_seconds_total",
		"total seconds of all commands duration",
		[]string{"host", "role", "cmd"}, nil)
	text := rds.Client.Info("commandstats").String()
	lines := strings.Split(text, "\r\n")

	prefix := "cmdstat_"
	for _, line := range lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		cmd, calls, usec := parseCommandStats(rds, line)
		ch <- prometheus.MustNewConstMetric(callsDesc, prometheus.CounterValue, float64(calls), rds.Address, rds.Role, cmd)
		ch <- prometheus.MustNewConstMetric(durationDesc, prometheus.CounterValue, float64(usec)/1000000, rds.Address, rds.Role, cmd)
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
