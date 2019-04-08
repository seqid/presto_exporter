/**
 * Copyright (C) 2018 Yahoo Japan Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "presto_cluster"
)

var hostname, _ = os.Hostname()

type Exporter struct {
	uri string
}

type ClusterExporter struct {
	RunningQueries   float64 `json:"runningQueries"`
	BlockedQueries   float64 `json:"blockedQueries"`
	QueuedQueries    float64 `json:"queuedQueries"`
	ActiveWorkers    float64 `json:"activeWorkers"`
	RunningDrivers   float64 `json:"runningDrivers"`
	ReservedMemory   float64 `json:"reservedMemory"`
	TotalInputRows   float64 `json:"totalInputRows"`
	TotalInputBytes  float64 `json:"totalInputBytes"`
	TotalCpuTimeSecs float64 `json:"totalCpuTimeSecs"`
}

type InfoExporter struct {
	NodeVersion struct {
		Version string `json:"version"`
	} `json:"nodeVersion"`
	Environment string `json:"environment"`
	Coordinator bool   `json:"coordinator"`
	Starting    bool   `json:"starting"`
	Uptime      string `json:"uptime"`
}

type Query struct {
	QueryId    string `json:"queryId"`
	State      string `json:"state"`
	Scheduled  bool   `json:"scheduled"`
	Query      string `json:"query"`
	QueryStats struct {
		QueuedTime                string  `json:"queuedTime"`
		ElapsedTime               string  `json:"elapsedTime"`
		ExecutionTime             string  `json:"executionTime"`
		TotalDrivers              int     `json:"totalDrivers"`
		RawInputDataSize          string  `json:"rawInputDataSize"`
		CumulativeUserMemory      float64 `json:"cumulativeUserMemory"`
		PeakUserMemoryReservation string  `json:"peakUserMemoryReservation"`
		TotalCpuTime              string  `json:"totalCpuTime"`
		TotalScheduledTime        string  `json:"totalScheduledTime"`
	} `json:"queryStats"`
}

type QueryExporter struct {
	Querys []Query
}

var (
	runningQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_queries"),
		"Running requests of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	blockedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "blocked_queries"),
		"Blocked queries of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	queuedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queued_queries"),
		"Queued queries of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	activeWorkers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_workers"),
		"Active workers of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	runningDrivers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_drivers"),
		"Running drivers of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	reservedMemory = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "reserved_memory"),
		"Reserved memory of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	totalInputRows = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_rows"),
		"Total input rows of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	totalInputBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_bytes"),
		"Total input bytes of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	totalCpuTimeSecs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_cpu_time_secs"),
		"Total cpu time of the presto cluster.",
		[]string{"hostname"}, nil,
	)
	uptime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "uptime"),
		"Total up time of the presto cluster.",
		[]string{"hostname"}, nil,
	)

	querys = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "querys"),
		"Querys of the presto cluster.",
		[]string{"hostname", "queryId", "state", "scheduled", "query", "queuedTime", "elapsedTime", "executionTime", "totalDrivers", "rawInputDataSize", "cumulativeUserMemory", "PeakUserMemoryReservation", "totalCpuTime", "totalScheduledTime"}, nil,
	)
)

// Describe implements the prometheus.Collector interface.
func (e Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- runningQueries
	ch <- blockedQueries
	ch <- queuedQueries
	ch <- activeWorkers
	ch <- runningDrivers
	ch <- reservedMemory
	ch <- totalInputRows
	ch <- totalInputBytes
	ch <- totalCpuTimeSecs
	ch <- uptime
	ch <- querys
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address on which to expose metrics and web interface.").Default(":9483").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		opts          = Exporter{}
	)
	kingpin.Flag("web.url", "Presto cluster address.").Default("http://localhost:8080").StringVar(&opts.uri)

	log.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("presto_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.Infoln("Starting presto_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	prometheus.MustRegister(&Exporter{uri: opts.uri})

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Presto Exporter</title></head>
			<body>
			<h1>Presto Exporter</h1>
			<p><a href="` + *metricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

// Collect implements the prometheus.Collector interface.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// cluster
	clusterResp, err := http.Get(e.uri + "/v1/cluster")
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	if clusterResp.StatusCode != 200 {
		log.Errorf("%s", err)
		return
	}
	defer clusterResp.Body.Close()

	clusterBody, err := ioutil.ReadAll(clusterResp.Body)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	var clusterExporter = ClusterExporter{}
	err = json.Unmarshal(clusterBody, &clusterExporter)
	if err != nil {
		log.Errorf("%s", err)
		return
	}

	// info
	infoResp, err := http.Get(e.uri + "/v1/info")
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	if infoResp.StatusCode != 200 {
		log.Errorf("%s", err)
		return
	}
	defer infoResp.Body.Close()

	infoBody, err := ioutil.ReadAll(infoResp.Body)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	var infoExporter = InfoExporter{}
	err = json.Unmarshal(infoBody, &infoExporter)
	if err != nil {
		log.Errorf("%s", err)
		return
	}

	// query
	queryResp, err := http.Get(e.uri + "/v1/query")
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	if queryResp.StatusCode != 200 {
		log.Errorf("%s", err)
		return
	}
	defer queryResp.Body.Close()

	queryBody, err := ioutil.ReadAll(queryResp.Body)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	var queryExporter = QueryExporter{}
	err = json.Unmarshal(queryBody, &queryExporter.Querys)
	if err != nil {
		log.Errorf("%s", err)
		return
	}

	ch <- prometheus.MustNewConstMetric(runningQueries, prometheus.GaugeValue, clusterExporter.RunningQueries, hostname)
	ch <- prometheus.MustNewConstMetric(blockedQueries, prometheus.GaugeValue, clusterExporter.BlockedQueries, hostname)
	ch <- prometheus.MustNewConstMetric(queuedQueries, prometheus.GaugeValue, clusterExporter.QueuedQueries, hostname)
	ch <- prometheus.MustNewConstMetric(activeWorkers, prometheus.GaugeValue, clusterExporter.ActiveWorkers, hostname)
	ch <- prometheus.MustNewConstMetric(runningDrivers, prometheus.GaugeValue, clusterExporter.RunningDrivers, hostname)
	ch <- prometheus.MustNewConstMetric(reservedMemory, prometheus.GaugeValue, clusterExporter.ReservedMemory, hostname)
	ch <- prometheus.MustNewConstMetric(totalInputRows, prometheus.GaugeValue, clusterExporter.TotalInputRows, hostname)
	ch <- prometheus.MustNewConstMetric(totalInputBytes, prometheus.GaugeValue, clusterExporter.TotalInputBytes, hostname)
	ch <- prometheus.MustNewConstMetric(totalCpuTimeSecs, prometheus.GaugeValue, clusterExporter.TotalCpuTimeSecs, hostname)
	uptimeF, _ := strconv.ParseFloat(strings.TrimSuffix(infoExporter.Uptime, "d"), 32)
	ch <- prometheus.MustNewConstMetric(uptime, prometheus.GaugeValue, uptimeF, hostname)

	for _, v := range queryExporter.Querys {
		labels := []string{
			hostname,
			v.QueryId,
			v.State,
			strconv.FormatBool(v.Scheduled),
			v.Query,
			v.QueryStats.QueuedTime,
			v.QueryStats.ElapsedTime,
			v.QueryStats.ExecutionTime,
			strconv.Itoa(v.QueryStats.TotalDrivers),
			v.QueryStats.RawInputDataSize,
			strconv.FormatFloat(v.QueryStats.CumulativeUserMemory, 'E', -1, 32),
			v.QueryStats.PeakUserMemoryReservation,
			v.QueryStats.TotalCpuTime,
			v.QueryStats.TotalScheduledTime,
		}
		ch <- prometheus.MustNewConstMetric(querys, prometheus.GaugeValue, v.QueryStats.CumulativeUserMemory, labels...)
	}
}
