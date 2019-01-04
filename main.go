package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shafreeck/configo"
	"github.com/shafreeck/continuous"
)

type Conf struct {
	Listen  string   `cfg:"listen; :8804; netaddr; the listen address of the http server"`
	Path    string   `cfg:"path; /metrics;; the path to handle metrics requests"`
	Auth    string   `cfg:"auth;;; password to connect to redis"`
	Masters []string `cfg:"masters;['127.0.0.1:6379'];; redis master addresses"`
}

func main() {
	c := &Conf{}

	if err := configo.Load("./redis-metrics.toml", c); err != nil {
		log.Fatalln("load configuration file failed", err)
	}
	log.Println(c)

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(
		NewRedisCollector(c),
	)

	mux := http.NewServeMux()
	mux.Handle(c.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	server := &http.Server{Addr: c.Listen, Handler: mux}

	cont := continuous.New()
	if err := cont.AddServer(continuous.WrapHTTPServer(server), &continuous.ListenOn{"tcp", c.Listen}); err != nil {
		log.Fatalln(err)
	}
	log.Fatalln(cont.Serve())
}
