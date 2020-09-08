package vistar

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type cacheCall struct {
	url string
	ech chan error
	och chan string
}

type eventCall struct {
	name    string
	message string
	source  string
	level   string
}

func TestCacheAdsNoCacher(t *testing.T) {
	resp := &AdResponse{
		Advertisement: []Ad{
			map[string]interface{}{"asset_url": "url1"},
			map[string]interface{}{"asset_url": "url2"},
		},
	}

	config := &ClientConfig{
		ReqTimeout: time.Second,
		EventFn:    nil,
		CacheFn:    nil,
		AssetTTL:   time.Minute,
	}
	client := NewClient(config)

	client.cacheAds(resp)

	assert.Len(t, client.inProgressAds, 0)
	assert.Len(t, resp.Advertisement, 2)
	assert.Equal(t, resp.Advertisement[0]["asset_url"], "url1")
	assert.Equal(t, resp.Advertisement[1]["asset_url"], "url2")
}

func TestCacheAds(t *testing.T) {
	resp := &AdResponse{
		Advertisement: []Ad{
			map[string]interface{}{"id": "1", "asset_url": "url1"},
			map[string]interface{}{"id": "2", "asset_url": "url2"},
		},
	}

	cacheCalls := make(chan *cacheCall, 100)
	cacheFn := func(url string, ttl time.Duration) (string, error) {
		ech := make(chan error)
		och := make(chan string)

		cacheCalls <- &cacheCall{
			ech: ech,
			och: och,
			url: url}

		for {
			select {
			case err := <-ech:
				return "", err
			case p := <-och:
				return p, nil
			}
		}
	}

	eventCalls := make([]*eventCall, 0, 0)
	eventFn := func(name string, message string, source string, level string) {
		eventCalls = append(eventCalls, &eventCall{
			name:    name,
			message: message,
			source:  source,
			level:   level})
	}

	config := &ClientConfig{
		ReqTimeout: time.Second,
		EventFn:    eventFn,
		CacheFn:    cacheFn,
		AssetTTL:   time.Second,
	}
	client := NewClient(config)

	cacheError := errors.New("cache failed")
	cacheEntry := "/cached-url"
	done := make(chan bool)
	go func() {
		client.cacheAds(resp)
		assert.Len(t, resp.Advertisement, 2)
		assert.Equal(t, resp.Advertisement[0]["asset_url"], "url1")
		assert.Equal(t, resp.Advertisement[0]["should_expire"], true)

		assert.Equal(t, resp.Advertisement[1]["asset_url"], "/cached-url")
		assert.Equal(t, resp.Advertisement[1]["original_asset_url"], "url2")
		_, ok := resp.Advertisement[1]["should_expire"]
		assert.False(t, ok)

		assert.Len(t, client.inProgressAds, 1)
		assert.Equal(t, client.inProgressAds["2"]["asset_url"], "/cached-url")

		assert.Len(t, eventCalls, 1)
		assert.Equal(t, eventCalls[0].name, "app-cache-failed")
		done <- true
	}()

	go func() {
		call := <-cacheCalls
		// Fail to cache url1
		if call.url == "url1" {
			call.ech <- cacheError
		} else {
			call.och <- cacheEntry
		}
		// Cache url2 as /cached-url
		call = <-cacheCalls
		if call.url == "url1" {
			call.ech <- cacheError
		} else {
			call.och <- cacheEntry
		}
	}()

	<-done
}

func TestTryToExpireAdsAllValid(t *testing.T) {
	resp := &AdResponse{
		Advertisement: []Ad{
			map[string]interface{}{"id": "1", "asset_url": "url1"},
			map[string]interface{}{"id": "2", "asset_url": "url2"},
		},
	}

	pop := NewTestProofOfPlay()
	client := &client{pop: pop}

	nresp := client.tryToExpireAds(resp)
	assert.Len(t, nresp.Advertisement, 2)
	assert.Equal(t, nresp.Advertisement[0]["id"], "1")
	assert.Equal(t, nresp.Advertisement[1]["id"], "2")

	assert.Len(t, pop.requests, 0)
}

func TestTryToExpireAds(t *testing.T) {
	resp := &AdResponse{
		Advertisement: []Ad{
			map[string]interface{}{"id": "1", "asset_url": "url1",
				"should_expire": true},
			map[string]interface{}{"id": "2", "asset_url": "url2",
				"should_expire": false},
		},
	}

	pop := NewTestProofOfPlay()
	client := &client{pop: pop}

	nresp := client.tryToExpireAds(resp)
	assert.Len(t, nresp.Advertisement, 1)
	assert.Equal(t, nresp.Advertisement[0]["id"], "2")

	assert.Len(t, pop.requests, 1)
	assert.Equal(t, pop.requests[0].Ad["asset_url"], "url1")
}

func TestRemoveExpiredAds(t *testing.T) {
	ad1 := map[string]interface{}{
		"id":           "1",
		"asset_url":    "url1",
		"lease_expiry": float64(12345),
	}
	ad2 := map[string]interface{}{
		"id":           "2",
		"asset_url":    "url1",
		"lease_expiry": float64(time.Now().Unix() + int64(1000)),
	}

	inProgressAds := make(map[string]Ad)
	inProgressAds[ad1["id"].(string)] = ad1
	inProgressAds[ad2["id"].(string)] = ad2

	client := &client{
		inProgressAds: inProgressAds,
	}

	assert.Equal(t, len(client.inProgressAds), 2)

	client.removeExpiredAds()

	assert.Equal(t, len(client.inProgressAds), 1)
	assert.NotContains(t, client.inProgressAds, ad1["id"].(string))
	assert.Contains(t, client.inProgressAds, ad2["id"].(string))
}

func TestUpdateBandwidthStats(t *testing.T) {
	pop := NewTestProofOfPlay()
	client := &client{
		pop:            pop,
		bandwidthStats: make(map[string]Stats),
	}

	client.updateBandwidthStats("/test", int64(100), int64(1024))

	stats, ok := client.bandwidthStats["/test"]
	assert.True(t, ok)

	assert.Equal(t, stats.Count, int64(1))
	assert.Equal(t, stats.BytesSent, int64(100))
	assert.Equal(t, stats.BytesReceived, int64(1024))
	assert.Equal(t, stats.Total, int64(1124))
	assert.Equal(t, stats.Average, float64(1124))

	client.updateBandwidthStats("/test", int64(100), int64(1024))

	stats, ok = client.bandwidthStats["/test"]

	assert.True(t, ok)
	assert.Equal(t, stats.Count, int64(2))
	assert.Equal(t, stats.BytesSent, int64(200))
	assert.Equal(t, stats.BytesReceived, int64(2048))
	assert.Equal(t, stats.Total, int64(2248))
	assert.Equal(t, stats.Average, float64(1124))
}

func TestPostWithMissingRequestData(t *testing.T) {
	request := &request{}
	pop := NewTestProofOfPlay()
	client := &client{
		pop: pop,
	}

	resp, err := client.post("test.com", request)

	assert.Empty(t, resp)
	assert.Equal(t, err, MissingRequestData)
}

func TestPostToInvalidRequestUrl(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		}),
	)
	defer ts.Close()
	data := &Data{}
	request := &request{
		data: data,
	}
	pop := NewTestProofOfPlay()
	client := &client{
		pop:        pop,
		httpClient: ts.Client(),
	}

	url := fmt.Sprintf("invalid%s", ts.URL)

	resp, err := client.post(url, request)

	assert.Empty(t, resp)
	assert.NotEmpty(t, err)
	assert.Equal(t, err.Error(),
		fmt.Sprintf("Post %s: unsupported protocol scheme \"invalidhttp\"",
			url))
}

func TestPostServerReturnHttpError(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}),
	)
	defer ts.Close()

	data := &Data{}
	request := &request{
		data: data,
	}
	pop := NewTestProofOfPlay()

	client := &client{
		pop:            pop,
		httpClient:     ts.Client(),
		bandwidthStats: make(map[string]Stats),
	}

	resp, err := client.post(ts.URL, request)

	expectedErrorMessage := fmt.Sprintf(
		"Ad server returned an error. url: %s, code: %d, body: ",
		ts.URL, http.StatusBadRequest)

	assert.Empty(t, resp)
	assert.NotEmpty(t, err)
	assert.Equal(t, err.Error(), expectedErrorMessage)
}

func TestPostServerReturnedHttpOK(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Success!"))
		}),
	)
	defer ts.Close()

	data := &Data{}
	request := &request{
		data: data,
	}
	pop := NewTestProofOfPlay()

	client := &client{
		pop:            pop,
		httpClient:     ts.Client(),
		bandwidthStats: make(map[string]Stats),
	}

	resp, err := client.post(ts.URL, request)

	assert.NotEmpty(t, resp)
	assert.Empty(t, err)
	assert.Equal(t, string(resp), "Success!")
}

func TestGetAdReturnsErrorWhenServerReturnsError(t *testing.T) {
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}),
	)
	defer ts.Close()

	data := &Data{}
	request := &request{
		url:  ts.URL,
		data: data,
	}
	pop := NewTestProofOfPlay()

	client := &client{
		pop:            pop,
		httpClient:     ts.Client(),
		bandwidthStats: make(map[string]Stats),
	}

	resp, err := client.GetAd(request)

	expectedErrorMessage := fmt.Sprintf(
		"Ad server returned an error. url: %s, code: %d, body: ",
		ts.URL, http.StatusBadRequest)

	assert.Empty(t, resp)
	assert.NotEmpty(t, err)
	assert.Equal(t, err.Error(), expectedErrorMessage)
}

func TestGetAdReturnsAd(t *testing.T) {
	adResponse := &AdResponse{
		[]Ad{
			{
				"id":        "1",
				"asset-url": "asset-url.ad-server.com",
			},
		},
	}

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			response, _ := json.Marshal(adResponse)
			w.Write(response)
		}),
	)
	defer ts.Close()

	data := &Data{}
	request := &request{
		url:  ts.URL,
		data: data,
	}

	config := &ClientConfig{
		ReqTimeout: time.Second * 100,
		EventFn:    nil,
		CacheFn:    nil,
		AssetTTL:   time.Second * 100,
	}
	client := NewClientForTesting(config, time.Millisecond*50)

	resp, err := client.GetAd(request)

	assert.NotEmpty(t, resp)
	assert.Equal(t, resp, adResponse)
	assert.Empty(t, err)

	assert.Equal(t, len(client.GetInProgressAds()), 1)
	assert.Equal(t, client.GetInProgressAds(),
		map[string]Ad{
			"1": map[string]interface{}{
				"id": "1", "asset-url": "asset-url.ad-server.com"},
		},
	)
}

func TestGetAdReturnsMultipleAds(t *testing.T) {
	adResponse := &AdResponse{
		[]Ad{
			{
				"id":        "1",
				"asset-url": "asset-url.ad-server.com",
			},
			{
				"id":        "2",
				"asset-url": "asset-url.ad-server.com",
			},
			{
				"id":        "3",
				"asset-url": "asset-url.ad-server.com",
			},
		},
	}

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			response, _ := json.Marshal(adResponse)
			w.Write(response)
		}),
	)
	defer ts.Close()

	data := &Data{}
	request := &request{
		url:  ts.URL,
		data: data,
	}

	config := &ClientConfig{
		ReqTimeout: time.Second * 100,
		EventFn:    nil,
		CacheFn:    nil,
		AssetTTL:   time.Second * 100,
	}
	client := NewClientForTesting(config, time.Millisecond*50)

	resp, err := client.GetAd(request)

	assert.NotEmpty(t, resp)
	assert.Equal(t, resp, adResponse)
	assert.Empty(t, err)

	assert.Equal(t, len(client.GetInProgressAds()), 3)
	assert.Equal(t, client.GetInProgressAds(),
		map[string]Ad{
			"1": map[string]interface{}{
				"id": "1", "asset-url": "asset-url.ad-server.com"},
			"2": map[string]interface{}{
				"id": "2", "asset-url": "asset-url.ad-server.com"},
			"3": map[string]interface{}{
				"id": "3", "asset-url": "asset-url.ad-server.com"},
		})
}

func TestStopClient(t *testing.T) {
	ad1 := map[string]interface{}{
		"id":           "1",
		"asset_url":    "url1",
		"lease_expiry": float64(12345),
	}
	ad2 := map[string]interface{}{
		"id":           "2",
		"asset_url":    "url2",
		"lease_expiry": float64(time.Now().Unix() + int64(1)),
	}

	inProgressAds := make(map[string]Ad)
	inProgressAds[ad1["id"].(string)] = ad1
	inProgressAds[ad2["id"].(string)] = ad2

	config := &ClientConfig{
		ReqTimeout: time.Second * 100,
		EventFn:    nil,
		CacheFn:    nil,
		AssetTTL:   time.Second * 100,
	}
	client := NewClientForTesting(config, time.Millisecond*50)
	client.inProgressAds = inProgressAds

	ads := client.GetInProgressAds()
	assert.Equal(t, len(ads), 2)

	time.Sleep(20 * time.Millisecond)

	ads = client.GetInProgressAds()
	assert.Equal(t, len(ads), 2)
	assert.Contains(t, ads, ad1["id"].(string))
	assert.Contains(t, ads, ad2["id"].(string))

	// processExpiredAds timer will be active.
	time.Sleep(40 * time.Millisecond)

	ads = client.GetInProgressAds()
	assert.Equal(t, len(ads), 1)
	assert.NotContains(t, ads, ad1["id"].(string))
	assert.Contains(t, ads, ad2["id"].(string))

	// Close the client. This should stop the processExpiredAds() goroutine.
	client.Close()

	// processExpiredAds timer should not be active.
	time.Sleep(40 * time.Millisecond)

	ads = client.GetInProgressAds()
	assert.Equal(t, len(ads), 1)
	assert.NotContains(t, ads, ad1["id"].(string))
	assert.Contains(t, ads, ad2["id"].(string))

	// processExpiredAds timer should not be active.
	time.Sleep(1000 * time.Millisecond)

	ads = client.GetInProgressAds()
	assert.Equal(t, len(ads), 1)
	assert.NotContains(t, ads, ad1["id"].(string))
	assert.Contains(t, ads, ad2["id"].(string))
}
