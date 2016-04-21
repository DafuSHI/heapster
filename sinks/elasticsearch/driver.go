// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"k8s.io/heapster/extpoints"
	"flag"
	sink_api "k8s.io/heapster/sinks/api"
	sinkutil "k8s.io/heapster/sinks/util"
	kube_api "k8s.io/kubernetes/pkg/api"
	"net/url"
	"time"
)
var (
	argTrunk        = flag.String("trunk", "", "How long the index will be divided to")
)
const (
	timeSeriesIndex = "heapster-metrics"
	eventsIndex     = "heapster-events"
	typeName        = "k8s-heapster"
)

// LimitFunc is a pluggable function to enforce limits on the object
type SaveDataFunc func(esClient *elastic.Client, indexName string, typeName string, sinkData interface{}) error

type elasticSearchSink struct {
	esClient        *elastic.Client
	saveDataFunc    SaveDataFunc
	timeSeriesIndex string
	eventsIndex     string
	needAuthen      bool
	esUserName      string
	esUserSecret    string
	esNodes         []string
	ci              sinkutil.ClientInitializer
}

type EsSinkPoint struct {
	MetricsName      string
	MetricsValue     interface{}
	MetricsTimestamp time.Time
	MetricsTags      map[string]string
}

type EsSinkEvent struct {
	EventMessage        string
	EventReason         string
	EventTimestamp      time.Time
	EventCount          int
	EventInvolvedObject interface{}
	EventSource         interface{}
}

// START: ExternalSink interface implementations

func (esSink *elasticSearchSink) Register(mds []sink_api.MetricDescriptor) error {
	return nil
}

func (esSink *elasticSearchSink) Unregister(mds []sink_api.MetricDescriptor) error {
	return nil
}

func (esSink *elasticSearchSink) StoreTimeseries(timeseries []sink_api.Timeseries) error {
	if !esSink.ci.Done() || timeseries == nil || len(timeseries) <= 0 {
		return nil
	}
	for _, t := range timeseries {
		seriesName := t.Point.Name
		if t.MetricDescriptor.Units.String() != "" {
			seriesName = fmt.Sprintf("%s_%s", seriesName, t.MetricDescriptor.Units.String())
		}
		if t.MetricDescriptor.Type.String() != "" {
			seriesName = fmt.Sprintf("%s_%s", seriesName, t.MetricDescriptor.Type.String())
		}
		sinkPoint := EsSinkPoint{
			MetricsName:      seriesName,
			MetricsValue:     t.Point.Value,
			MetricsTimestamp: t.Point.End.UTC(),
			MetricsTags:      make(map[string]string, len(t.Point.Labels)),
		}
		for key, value := range t.Point.Labels {
			if value != "" {
				sinkPoint.MetricsTags[key] = value
			}
		}
		//add a variable
		var Indextrunck string
		var err error
		if *argTrunk == "true" {
			//glog.Infof("the trunk is %s for metrics", *argTrunk)
			Indextrunck = esSink.timeSeriesIndex + "_" + fmt.Sprintf(time.Now().Format("20060102"))
			err = esSink.saveDataFunc(esSink.esClient, Indextrunck, typeName, sinkPoint)
		} else {
			err = esSink.saveDataFunc(esSink.esClient, esSink.timeSeriesIndex, typeName, sinkPoint)
		}
		
		if err != nil {
			return fmt.Errorf("failed to save metrics to ES cluster: %s", err)
		}
	}
	return nil
}

func (esSink *elasticSearchSink) StoreEvents(events []kube_api.Event) error {
	if !esSink.ci.Done() || events == nil || len(events) <= 0 {
		return nil
	}
	for _, event := range events {
		sinkEvent := EsSinkEvent{
			EventMessage:        event.Message,
			EventReason:         event.Reason,
			EventTimestamp:      event.LastTimestamp.UTC(),
			EventCount:          event.Count,
			EventInvolvedObject: event.InvolvedObject,
			EventSource:         event.Source,
		}
		var Indextrunck string
		var err error
		if *argTrunk == "true" {
			//glog.Infof("the trunk is %s for event", *argTrunk)
			Indextrunck = esSink.eventsIndex + "_" + fmt.Sprintf(time.Now().Format("20060102"))
			err = esSink.saveDataFunc(esSink.esClient, Indextrunck, typeName, sinkEvent)
		} else {
			err = esSink.saveDataFunc(esSink.esClient, esSink.eventsIndex, typeName, sinkEvent)
		}
		//Indextrunck = esSink.eventsIndex + "_" + fmt.Sprintf(time.Now().Format("2006-01-02_15:04"))
		//err := esSink.saveDataFunc(esSink.esClient, Indextrunck, typeName, sinkEvent)

		if err != nil {
			return fmt.Errorf("failed to save events to ES cluster: %s", err)
		}
	}
	return nil
}

//SaveDataIntoES save metrics and events to ES by using ES client
func SaveDataIntoES(esClient *elastic.Client, indexName string, typeName string, sinkData interface{}) error {
	if indexName == "" || typeName == "" || sinkData == nil {
		return nil
	}
	// Use the IndexExists service to check if a specified index exists.
	exists, err := esClient.IndexExists(indexName).Do()
	if err != nil {
		return err
	}
	if !exists {
		// Create a new index.
		createIndex, err := esClient.CreateIndex(indexName).Do()
		if err != nil {
			return err
		}
		if !createIndex.Acknowledged {
			return fmt.Errorf("failed to create Index in ES cluster: %s", err)
		}
	}
	indexID := uuid.NewUUID()
	_, err = esClient.Index().
		Index(indexName).
		Type(typeName).
		Id(string(indexID)).
		BodyJson(sinkData).
		Do()
	if err != nil {
		return err
	}
	return nil
}

func (esSink *elasticSearchSink) DebugInfo() string {
	info := fmt.Sprintf("%s\n", esSink.Name())
	info += fmt.Sprintf("There are two elasticsearch index: %s,%s:\n", esSink.eventsIndex, esSink.timeSeriesIndex)
	info += fmt.Sprintf("Cluster's nodes list list is: %s", esSink.esNodes)
	if !esSink.ci.Done() {
		info += fmt.Sprintf("ElasticSearch client has not been initialized yet.")
	}
	return info
}

func (esSink *elasticSearchSink) Name() string {
	return "ElasticSearch Sink"
}

func (esSink *elasticSearchSink) ping() error {

	//The client should sniff both URLs
	var err error
	if esSink.needAuthen == false {
		_, err = elastic.NewClient(elastic.SetURL(esSink.esNodes...))
	} else {
		_, err = elastic.NewClient(elastic.SetBasicAuth(esSink.esUserName, esSink.esUserSecret), elastic.SetURL(esSink.esNodes...))
	}

	if err != nil {
		return fmt.Errorf("failed to connect any node of ES cluster from ping")
	}
	return nil
}

func (esSink *elasticSearchSink) setupClient() error {
	glog.V(3).Infof("attempting to setup elasticsearch sink")
	var err error
	var client *(elastic.Client)
	if esSink.needAuthen == false {
		client, err = elastic.NewClient(elastic.SetURL(esSink.esNodes...))
	} else {
		client, err = elastic.NewClient(elastic.SetBasicAuth(esSink.esUserName, esSink.esUserSecret), elastic.SetURL(esSink.esNodes...))
	}

	if err != nil {
		return fmt.Errorf("failed to connect any node of ES cluster during setupClient")
	}
	esSink.esClient = client
	glog.V(3).Infof("elasticsearch sink setup successfully")
	return nil
}

func init() {
	extpoints.SinkFactories.Register(NewElasticSearchSink, "elasticsearch")
}

func NewElasticSearchSink(uri *url.URL, _ extpoints.HeapsterConf) ([]sink_api.ExternalSink, error) {

	var esSink elasticSearchSink
	opts, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parser url's query string: %s", err)
	}

	//set the index for timeSeries,the default value is "timeSeriesIndex"
	esSink.timeSeriesIndex = timeSeriesIndex
	if len(opts["timeseriesIndex"]) > 0 {
		esSink.timeSeriesIndex = opts["timeseriesIndex"][0]
	}

	//set the index for eventsIndex, the default value is "eventsIndex"
	esSink.eventsIndex = eventsIndex
	if len(opts["eventsIndex"]) > 0 {
		esSink.eventsIndex = opts["eventsIndex"][0]
	}

	//If the ES cluster needs authentication, the username and secret
	//should be set in sink config
	//Else, set the Authenticate flag to false
	esSink.needAuthen = false
	if len(opts["esUserName"]) > 0 && len(opts["esUserSecret"]) > 0 {
		esSink.timeSeriesIndex = opts["esUserName"][0]
		esSink.eventsIndex = opts["esUserSecret"][0]
		esSink.needAuthen = true
	}

	//set the URL endpoints of the ES's nodes. Notice that
	// when sniffing is enabled, these URLs are used to initially sniff the
	// cluster on startup.
	if len(opts["nodes"]) < 1 {
		return nil, fmt.Errorf("There is no node assigned for connecting ES cluster")
	}
	esSink.esNodes = append(esSink.esNodes, opts["nodes"]...)
	glog.V(2).Infof("initializing elasticsearch sink with ES's nodes - %v", esSink.esNodes)

	esSink.saveDataFunc = SaveDataIntoES

	esSink.ci = sinkutil.NewClientInitializer("elasticsearch", esSink.setupClient, esSink.ping, 10*time.Second)
	return []sink_api.ExternalSink{&esSink}, nil
}
