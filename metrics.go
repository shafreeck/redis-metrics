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
			h, ok := rc.handlers[key]
			if !ok {
				continue
			}
			ch <- h(rds, val)
		}

		collectCommandStats(rds, ch)
	})
}

func NewRedisCollector(c *Conf) *RedisCollector {
	handlers := map[string]KeyHandler{
		"redis_version": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_version",
				"version of the redis instance",
				[]string{"host", "role", "version"}, nil)
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(1), rds.Address, rds.Role, val)
		},

		"uptime_in_seconds": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_up_seconds_total",
				"seconds since startup of the redis",
				[]string{"host", "role"}, nil)
			sec, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse uptime failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(sec), rds.Address, rds.Role)
		},
		"used_memory_rss": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_memory_usage_bytes",
				"redis memory usage",
				[]string{"host", "role", "type"}, nil)
			used, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse used_memory_rss failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(used), rds.Address, rds.Role, "rss")
		},
		"maxmemory": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_max_memory_bytes",
				"redis max memory configured, 0 means unlimited",
				[]string{"host", "role"}, nil)
			mem, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse maxmemory failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(mem), rds.Address, rds.Role)
		},
		"total_system_memory": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_memory_system_total_bytes",
				"total memory of the host",
				[]string{"host", "role"}, nil)
			mem, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse total_system_memory failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(mem), rds.Address, rds.Role)
		},
		"connected_clients": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_clients_connected_number",
				"number of redis clients",
				[]string{"host", "role"}, nil)
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse connected_clients failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(n), rds.Address, rds.Role)
		},
		"total_net_input_bytes": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_net_bytes_total",
				"network flowed bytes",
				[]string{"host", "role", "direction"}, nil)
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse total_net_input_bytes failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(n), rds.Address, rds.Role, "input")
		},
		"total_net_output_bytes": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_net_bytes_total",
				"network flowed bytes",
				[]string{"host", "role", "direction"}, nil)
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse total_net_output_bytes failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(n), rds.Address, rds.Role, "output")
		},
		"used_cpu_sys": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_cpu_usage_seconds_total",
				"total seconds of cpu time",
				[]string{"host", "role", "type"}, nil)
			used, err := strconv.ParseFloat(val, 64)
			if err != nil {
				log.Println("parse used_cpu_sys failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, used, rds.Address, rds.Role, "sys")
		},
		"used_cpu_user": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_cpu_usage_seconds_total",
				"total seconds of cpu time",
				[]string{"host", "role", "type"}, nil)
			used, err := strconv.ParseFloat(val, 64)
			if err != nil {
				log.Println("parse used_cpu_user failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, used, rds.Address, rds.Role, "user")
		},
		"total_commands_processed": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_commands_processed_total",
				"total number of commands has been processed",
				[]string{"host", "role"}, nil)
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse total_commands_processed failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(n), rds.Address, rds.Role)
		},
		"blocked_clients": func(rds *Redis, val string) prometheus.Metric {
			desc := prometheus.NewDesc(
				"redis_clients_blocked_number",
				"number of blocked clients",
				[]string{"host", "role"}, nil)
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Println("parse blocked_clients failed", err)
			}
			return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(n), rds.Address, rds.Role)
		},
	}

	return &RedisCollector{conf: c, handlers: handlers}
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
