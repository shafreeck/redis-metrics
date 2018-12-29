package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shafreeck/configo"
)

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

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":8804", nil))
}
