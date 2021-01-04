package vistar

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var AdNotFound = errors.New("ad not found")
var MissingRequestData = errors.New("missing request data")
var ProcessExpiredAdInterval = 1 * time.Minute

type CacheFunc func(string, time.Duration) (string, error)
type EventFunc func(string, string, string, string)
type PoPFunc func(string, string, *ProofOfPlayRequest) (
	*http.Response, error)

type Ad map[string]interface{}
type AdResponse struct {
	Advertisement []Ad `json:"advertisement,omitempty"`
}

type Asset map[string]interface{}
type AssetResponse struct {
	Assets []Asset `json:"asset,omitempty"`
}

type Client interface {
	GetAd(Request) (*AdResponse, error)
	Expire(string) error
	Confirm(string, int64) (string, error)
	GetInProgressAds() map[string]Ad
	GetAssets(Request) (*AssetResponse, error)
	GetStats() map[string]Stats
	Close()
}

type ClientConfig struct {
	ReqTimeout     time.Duration
	EventFn        EventFunc
	CacheFn        CacheFunc
	AssetTTL       time.Duration
	ExpiryInterval time.Duration
	PoPFn          PoPFunc
}

type client struct {
	httpClient       *http.Client
	pop              ProofOfPlay
	assetTTL         time.Duration
	cacheFn          CacheFunc
	eventFn          EventFunc
	lock             sync.RWMutex
	inProgressAds    map[string]Ad
	statsLock        sync.RWMutex
	bandwidthStats   map[string]Stats
	closeCh          chan struct{}
	adExpiryInterval time.Duration
}

func NewClientForTesting(config *ClientConfig,
	expiryInterval time.Duration) *client {
	httpClient := &http.Client{Timeout: config.ReqTimeout}
	pop := NewProofOfPlay(config.EventFn, config.PoPFn)

	c := &client{
		pop:              pop,
		assetTTL:         config.AssetTTL,
		httpClient:       httpClient,
		eventFn:          config.EventFn,
		cacheFn:          config.CacheFn,
		inProgressAds:    make(map[string]Ad),
		bandwidthStats:   make(map[string]Stats),
		closeCh:          make(chan struct{}, 1),
		adExpiryInterval: expiryInterval,
	}

	go c.processExpiredAds()
	return c
}

func NewClient(config *ClientConfig) *client {
	httpClient := &http.Client{Timeout: config.ReqTimeout}
	pop := NewProofOfPlay(config.EventFn, config.PoPFn)

	c := &client{
		pop:              pop,
		assetTTL:         config.AssetTTL,
		httpClient:       httpClient,
		eventFn:          config.EventFn,
		cacheFn:          config.CacheFn,
		inProgressAds:    make(map[string]Ad),
		bandwidthStats:   make(map[string]Stats),
		closeCh:          make(chan struct{}, 1),
		adExpiryInterval: ProcessExpiredAdInterval,
	}

	go c.processExpiredAds()
	return c
}

func (c *client) Close() {
	c.closeCh <- struct{}{}
}

func (c *client) GetStats() map[string]Stats {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()

	return c.bandwidthStats
}

func (c *client) GetInProgressAds() map[string]Ad {
	c.lock.RLock()
	defer c.lock.RUnlock()

	ret := map[string]Ad{}
	for k, v := range c.inProgressAds {
		ret[k] = v
	}
	return ret
}

func (c *client) Expire(adId string) error {
	ad, ok := c.removeFromInProgressList(adId)
	if !ok {
		return AdNotFound
	}

	err := c.pop.Expire(ad)
	return err
}

func (c *client) Confirm(adId string, displayTime int64) (string, error) {
	ad, ok := c.removeFromInProgressList(adId)
	if !ok {
		return "", AdNotFound
	}

	err := c.pop.Confirm(ad, displayTime)
	return ad["original_asset_url"].(string), err
}

func (c *client) GetAd(request Request) (*AdResponse, error) {
	body, err := c.post(request.ServerUrl(), request)
	if err != nil {
		return nil, err
	}

	resp := &AdResponse{}
	err = json.Unmarshal(body, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.Advertisement) == 0 {
		c.publishEvent("ad-server-returned-no-ads", "", "warning")
		return resp, nil
	}

	if c.cacheFn != nil {
		c.cacheAds(resp)
	} else {
		for _, ad := range resp.Advertisement {
			c.addToInProgressList(ad)
		}
	}

	cleanedResponse := c.tryToExpireAds(resp)
	return cleanedResponse, nil
}

func (c *client) GetAssets(request Request) (*AssetResponse, error) {
	body, err := c.post(request.AssetEndpointUrl(), request)
	if err != nil {
		return nil, err
	}

	resp := &AssetResponse{}
	err = json.Unmarshal(body, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.Assets) == 0 {
		c.publishEvent("ad-server-returned-no-assets", "", "warning")
	}

	return resp, nil
}

func (c *client) post(url string, request Request) ([]byte, error) {
	reqData := request.Data()
	if reqData == nil {
		return nil, MissingRequestData
	}

	data, err := json.Marshal(reqData)
	if err != nil {
		return nil, err
	}

	hreq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	hreq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	c.updateBandwidthStats(
		url, getRequestLength(hreq), getResponseLength(resp))

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusBadRequest {
		c.publishEvent(
			"ad-server-endpoint-failed",
			fmt.Sprintf("url: %s, code: %d, body: %s", url, resp.StatusCode,
				string(body)),
			"warning")
		return nil, fmt.Errorf("Ad server returned an error. url: %s, "+
			"code: %d, body: %s", url, resp.StatusCode, string(body))
	}
	return body, err
}

func (c *client) cacheAds(resp *AdResponse) {
	if c.cacheFn == nil {
		return
	}
	var wg sync.WaitGroup
	for _, ad := range resp.Advertisement {
		originalUrl := ad["asset_url"].(string)
		wg.Add(1)
		go func(ad Ad) {
			defer wg.Done()
			local, err := c.cacheFn(originalUrl, c.assetTTL)
			if err != nil {
				c.publishEvent("app-cache-failed",
					fmt.Sprintf("url: %s, error: %s", originalUrl, err.Error()),
					"warning")
				ad["should_expire"] = true
				return
			}
			ad["original_asset_url"] = originalUrl
			ad["asset_url"] = local
			c.addToInProgressList(ad)
		}(ad)
	}
	wg.Wait()
}

func (c *client) tryToExpireAds(resp *AdResponse) *AdResponse {
	cleaned := &AdResponse{}
	for _, ad := range resp.Advertisement {
		shouldExpire, ok := ad["should_expire"].(bool)
		if ok && shouldExpire {
			c.pop.Expire(ad)
			continue
		}
		cleaned.Advertisement = append(cleaned.Advertisement, ad)
	}
	return cleaned
}

func (c *client) addToInProgressList(ad Ad) {
	c.lock.Lock()
	defer c.lock.Unlock()

	adId := ad["id"].(string)
	c.inProgressAds[adId] = ad
}

func (c *client) removeFromInProgressList(adId string) (Ad, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ad, ok := c.inProgressAds[adId]
	delete(c.inProgressAds, adId)
	return ad, ok
}

func (c client) publishEvent(name string, message string, level string) {
	if c.eventFn == nil {
		return
	}
	c.eventFn(name, message, "", level)
}

func (c *client) updateBandwidthStats(url string, sentBytes int64,
	receivedBytes int64) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()

	urlStats := c.bandwidthStats[url]
	updateStats(&urlStats, sentBytes, receivedBytes)
	c.bandwidthStats[url] = urlStats
}

func (c *client) processExpiredAds() {
	ticker := time.NewTicker(c.adExpiryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.removeExpiredAds()
		case <-c.closeCh:
			return
		}
	}
}

func (c *client) removeExpiredAds() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for adId, ad := range c.inProgressAds {
		leaseExpirySecond, ok := ad["lease_expiry"]
		if !ok {
			continue
		}

		// We are dropping the expired ad here and not expiring,
		// because ad server expires them automatically after 24hrs.
		if int64(leaseExpirySecond.(float64)) <= time.Now().Unix() {
			delete(c.inProgressAds, adId)
		}
	}
}
