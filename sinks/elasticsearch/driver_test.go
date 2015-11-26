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
	"encoding/json"
	"testing"
	"time"

	"fmt"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
	sink_api "k8s.io/heapster/sinks/api"
	sinkutil "k8s.io/heapster/sinks/util"
	kube_api "k8s.io/kubernetes/pkg/api"
	kube_api_unv "k8s.io/kubernetes/pkg/api/unversioned"
)

type dataSavedToES struct {
	data string
}

type fakeESSink struct {
	elasticSearchSink
	savedData []dataSavedToES
}

var FakeESSink fakeESSink

func SaveDataIntoES_Stub(esClient *elastic.Client, indexName string, typeName string, sinkData interface{}) error {
	jsonItems, err := json.Marshal(sinkData)
	if err != nil {
		return fmt.Errorf("failed to transform the items to json : %s", err)
	}
	FakeESSink.savedData = append(FakeESSink.savedData, dataSavedToES{string(jsonItems)})
	return nil
}

// Returns a fake ES sink.
func NewFakeSink() {
	var ESClient elastic.Client
	fakeSinkESNodes := make([]string, 2)
	savedData := make([]dataSavedToES, 0)
	FakeESSink = fakeESSink{
		elasticSearchSink{
			esClient:        &ESClient,
			saveDataFunc:    SaveDataIntoES_Stub,
			timeSeriesIndex: "heapster-metric-index",
			eventsIndex:     "heapster-events-index",
			needAuthen:      false,
			esUserName:      "admin",
			esUserSecret:    "admin",
			esNodes:         fakeSinkESNodes,
			ci:              sinkutil.NewClientInitializer("test", func() error { return nil }, func() error { return nil }, time.Millisecond),
		},
		savedData,
	}
}

func TestStoreEventsAndTimeseries(t *testing.T) {
	//Testcase1: Events Empty Input
	NewFakeSink()
	err := FakeESSink.StoreEvents([]kube_api.Event{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(FakeESSink.savedData))

	//Testcase2: Events Single Input
	NewFakeSink()
	eventTime := kube_api_unv.Unix(12345, 0)
	eventSourceHostname := "event1HostName"
	eventReason := "event1"
	events := []kube_api.Event{
		{
			Reason:        eventReason,
			LastTimestamp: eventTime,
			Source: kube_api.EventSource{
				Host: eventSourceHostname,
			},
		},
	}
	timeStr, err := eventTime.MarshalJSON()
	assert.NoError(t, err)

	msgString := fmt.Sprintf(`{"EventMessage":"","EventReason":"%s","EventTimestamp":%s,"EventCount":0,"EventInvolvedObject":{},"EventSource":{"host":"%s"}}`, eventReason, string(timeStr), eventSourceHostname)
	err = FakeESSink.StoreEvents(events)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(FakeESSink.savedData))
	assert.Equal(t, msgString, FakeESSink.savedData[0].data)

	//Testcase3: Multiple Events Input
	NewFakeSink()
	eventTime = kube_api_unv.Unix(12345, 0)
	event1SourceHostname := "event1HostName"
	event2SourceHostname := "event2HostName"
	event1Reason := "eventReason1"
	event2Reason := "eventReason2"
	events = []kube_api.Event{
		{
			Reason:        event1Reason,
			LastTimestamp: eventTime,
			Source: kube_api.EventSource{
				Host: event1SourceHostname,
			},
		},
		{
			Reason:        event2Reason,
			LastTimestamp: eventTime,
			Source: kube_api.EventSource{
				Host: event2SourceHostname,
			},
		},
	}
	err = FakeESSink.StoreEvents(events)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(FakeESSink.savedData))

	timeStr, err = eventTime.MarshalJSON()
	assert.NoError(t, err)

	msgString1 := fmt.Sprintf(`{"EventMessage":"","EventReason":"%s","EventTimestamp":%s,"EventCount":0,"EventInvolvedObject":{},"EventSource":{"host":"%s"}}`, event1Reason, string(timeStr), event1SourceHostname)
	assert.Equal(t, msgString1, FakeESSink.savedData[0].data)

	msgString2 := fmt.Sprintf(`{"EventMessage":"","EventReason":"%s","EventTimestamp":%s,"EventCount":0,"EventInvolvedObject":{},"EventSource":{"host":"%s"}}`, event2Reason, string(timeStr), event2SourceHostname)
	assert.Equal(t, msgString2, FakeESSink.savedData[1].data)

	//Testcase4: Timeseries Empty Input
	NewFakeSink()
	err = FakeESSink.StoreTimeseries([]sink_api.Timeseries{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(FakeESSink.savedData))

	//Testcase5: Timeseries Single Input
	NewFakeSink()
	smd := sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	l := make(map[string]string)
	l["test"] = "notvisible"
	l[sink_api.LabelHostname.Key] = "localhost"
	l[sink_api.LabelContainerName.Key] = "docker"
	l[sink_api.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"
	timeNow := time.Now()

	p := sink_api.Point{
		Name:   "test/metric/1",
		Labels: l,
		Start:  timeNow,
		End:    timeNow,
		Value:  int64(123456),
	}

	timeseries := []sink_api.Timeseries{
		{
			MetricDescriptor: &smd,
			Point:            &p,
		},
	}

	err = FakeESSink.StoreTimeseries(timeseries)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(FakeESSink.savedData))

	timeStr, err = timeNow.UTC().MarshalJSON()
	assert.NoError(t, err)

	msgString = fmt.Sprintf(`{"MetricsName":"test/metric/1_cumulative","MetricsValue":123456,"MetricsTimestamp":%s,"MetricsTags":{"container_name":"docker","hostname":"localhost","pod_id":"aaaa-bbbb-cccc-dddd","test":"notvisible"}}`, timeStr)

	assert.Equal(t, msgString, FakeESSink.savedData[0].data)

	//Testcase6: Multiple Timeseries Input
	NewFakeSink()
	smd = sink_api.MetricDescriptor{
		ValueType: sink_api.ValueInt64,
		Type:      sink_api.MetricCumulative,
	}

	p1 := sink_api.Point{
		Name:   "test/metric/1",
		Labels: l,
		Start:  timeNow,
		End:    timeNow,
		Value:  int64(123456),
	}

	p2 := sink_api.Point{
		Name:   "test/metric/1",
		Labels: l,
		Start:  timeNow,
		End:    timeNow,
		Value:  int64(654321),
	}

	timeseries2 := []sink_api.Timeseries{
		{
			MetricDescriptor: &smd,
			Point:            &p1,
		},
		{
			MetricDescriptor: &smd,
			Point:            &p2,
		},
	}

	err = FakeESSink.StoreTimeseries(timeseries2)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(FakeESSink.savedData))

	timeStr, err = timeNow.UTC().MarshalJSON()
	assert.NoError(t, err)

	msgString1 = fmt.Sprintf(`{"MetricsName":"test/metric/1_cumulative","MetricsValue":123456,"MetricsTimestamp":%s,"MetricsTags":{"container_name":"docker","hostname":"localhost","pod_id":"aaaa-bbbb-cccc-dddd","test":"notvisible"}}`, timeStr)
	assert.Equal(t, msgString1, FakeESSink.savedData[0].data)

	msgString2 = fmt.Sprintf(`{"MetricsName":"test/metric/1_cumulative","MetricsValue":654321,"MetricsTimestamp":%s,"MetricsTags":{"container_name":"docker","hostname":"localhost","pod_id":"aaaa-bbbb-cccc-dddd","test":"notvisible"}}`, timeStr)
	assert.Equal(t, msgString2, FakeESSink.savedData[1].data)
}
