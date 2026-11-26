package main

import (
	"net/http"
	"testing"
	"time"
)

func TestPublicServerBoundsSlowClients(t *testing.T) {
	srv := newPublicServer(":0", http.NewServeMux())
	if srv.ReadHeaderTimeout != 10*time.Second || srv.ReadTimeout != 30*time.Second || srv.WriteTimeout != 30*time.Second || srv.IdleTimeout != 120*time.Second {
		t.Fatalf("public server timeouts = header %v read %v write %v idle %v", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.WriteTimeout, srv.IdleTimeout)
	}
}

func TestSandboxServerKeepsStreamingWritesUnbounded(t *testing.T) {
	srv := newSandboxServer(":0", http.NewServeMux())
	if srv.ReadHeaderTimeout != 10*time.Second || srv.IdleTimeout != 120*time.Second {
		t.Fatalf("sandbox server timeouts = header %v idle %v", srv.ReadHeaderTimeout, srv.IdleTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Fatalf("sandbox server must not cut long SSE responses, got WriteTimeout %v", srv.WriteTimeout)
	}
}
