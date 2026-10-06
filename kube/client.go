package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
		return nil, fmt.Errorf("not running in a cluster")
	}
	if _, err := os.Stat(tokenPath); err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("parse service account ca")
	}
	return &Client{
		BaseURL: "https://" + host + ":" + port,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			},
		},
	}, nil
}

func (c *Client) Get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if token, err := os.ReadFile(tokenPath); err == nil {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: http %d", path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
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
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: http %d", path, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

type Pod struct {
	Metadata struct {
		Name  string `json:"name"`
		Owner []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		NodeName   string `json:"nodeName"`
		Containers []struct {
			Name         string          `json:"name"`
			Resources    json.RawMessage `json:"resources"`
			RestartCount int             `json:"restartCount"`
			LastState    json.RawMessage `json:"lastState"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase             string `json:"phase"`
		Reason            string `json:"reason"`
		ContainerStatuses []struct {
			Name         string `json:"name"`
			Ready        bool   `json:"ready"`
			RestartCount int    `json:"restartCount"`
			LastState    struct {
				Terminated struct {
					Reason   string `json:"reason"`
					ExitCode int    `json:"exitCode"`
					Signal   int    `json:"signal"`
				} `json:"terminated"`
			} `json:"lastState"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (c *Client) PodsInNamespace(ctx context.Context, namespace, selector string) ([]Pod, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", namespace, urlEscape(selector))
	var list struct {
		Items []Pod `json:"items"`
	}
	if err := c.Get(ctx, path, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) NodeMetrics(ctx context.Context, node, path string) ([]byte, error) {
	return c.Raw(ctx, fmt.Sprintf("/api/v1/nodes/%s/proxy/%s", node, strings.TrimPrefix(path, "/")))
}

func urlEscape(s string) string {
	replacer := strings.NewReplacer(
		"%", "%25",
		" ", "%20",
		",", "%2C",
		"=", "%3D",
		"+", "%2B",
		"/", "%2F",
	)
	return replacer.Replace(s)
}
