package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func InCluster() (*Client, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("parse service account ca")
	}
	return &Client{
		BaseURL: "https://" + host + ":" + port,
		HTTP: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

func (c *Client) Raw(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if token, err := os.ReadFile(tokenPath); err == nil {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: http %d", path, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) Get(ctx context.Context, path string, out any) error {
	body, err := c.Raw(ctx, path)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

type Pod struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
}

func (c *Client) PodsInNamespace(ctx context.Context, namespace, selector string) ([]Pod, error) {
	body, err := c.Raw(ctx, "/api/v1/namespaces/"+namespace+"/pods?labelSelector="+url.QueryEscape(selector))
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []Pod `json:"items"`
	}
	return list.Items, json.Unmarshal(body, &list)
}

func (c *Client) NodeMetrics(ctx context.Context, node, path string) ([]byte, error) {
	return c.Raw(ctx, "/api/v1/nodes/"+node+"/proxy/"+path)
}
