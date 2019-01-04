package main

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

type Conf struct {
	Listen  string `cfg:listen; :8804; netaddr; the listen address of the http server`
	Auth    string
	Masters []string
}

type Redis struct {
	Client   *redis.Client
	Role     string
	Address  string
	State    string
	Children []*Redis
}

// ParseRedisGroup parses the master and/or slaves of the specific redis
func ParseRedisGroup(address string, auth string, role string) *Redis {
	var children []*Redis
	master := &Redis{Role: role, Address: address}
	c := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: auth,
	})
	master.Client = c

	text, err := c.Do("info", "replication").String()
	if err != nil {
		log.Println("fetch redis info failed", err)
	}

	lines := strings.Split(text, "\n")
	re := regexp.MustCompile("slave[0-9]*:ip=(.*),port=([0-9]+),state=([a-z]+).*")
	for _, line := range lines {
		if !strings.HasPrefix(line, "slave") {
			continue
		}
		matches := re.FindStringSubmatch(line)
		if len(matches) < 4 {
			continue
		}
		host, port, state := matches[1], matches[2], matches[3]

		children = append(children, &Redis{Address: fmt.Sprintf("%s:%s", host, port), State: state, Role: "slave"})
	}

	// FIXME it is weired here
	for i := range children {
		children[i] = ParseRedisGroup(children[i].Address, auth, children[i].Role)
	}

	master.Children = children

	return master
}

func ParseRedisTopology(addresses []string, auth string) []*Redis {
	var groups []*Redis
	for _, addr := range addresses {
		groups = append(groups, ParseRedisGroup(addr, auth, "master"))
	}
	return groups
}

func RunOnRedis(redises []*Redis, f func(rds *Redis)) {
	var wg sync.WaitGroup
	RunOnRedisAsync(&wg, redises, f)
	wg.Wait()
}

func RunOnRedisAsync(wg *sync.WaitGroup, redises []*Redis, f func(rds *Redis)) {
	for _, rds := range redises {
		wg.Add(1)

		go func(rds *Redis) {
			f(rds)
			rds.Client.Close()
			wg.Done()
		}(rds)
		RunOnRedisAsync(wg, rds.Children, f)
	}
}
