// Package core implements Chain Core and its API.
package core

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"chain/core/accesstoken"
	"chain/core/account"
	"chain/core/asset"
	"chain/core/config"
	"chain/core/leader"
	"chain/core/mockhsm"
	"chain/core/pin"
	"chain/core/query"
	"chain/core/rpc"
	"chain/core/txbuilder"
	"chain/core/txdb"
	"chain/core/txfeed"
	"chain/database/pg"
	"chain/errors"
	"chain/generated/dashboard"
	"chain/generated/docs"
	"chain/net/http/gzip"
	"chain/net/http/httpjson"
	"chain/net/http/limit"
	"chain/net/http/reqid"
	"chain/net/http/static"
	"chain/protocol"
	"chain/protocol/bc"
)

const (
	defGenericPageSize = 100
)

// TODO(kr): change this to "network" or something.
const networkRPCPrefix = "/rpc/"

var (
	errNotFound       = errors.New("not found")
	errRateLimited    = errors.New("request limit exceeded")
	errLeaderElection = errors.New("no leader; pending election")
)

// Handler serves the Chain HTTP API
type Handler struct {
	Chain         *protocol.Chain
	Store         *txdb.Store
	PinStore      *pin.Store
	Assets        *asset.Registry
	Accounts      *account.Manager
	HSM           *mockhsm.HSM
	Indexer       *query.Indexer
	TxFeeds       *txfeed.Tracker
	AccessTokens  *accesstoken.CredentialStore
	Config        *config.Config
	DB            pg.DB
	Addr          string
	AltAuth       func(*http.Request) bool
	Signer        func(context.Context, *bc.Block) ([]byte, error)
	RequestLimits []RequestLimit

	once           sync.Once
	handler        http.Handler
	actionDecoders map[string]func(data []byte) (txbuilder.Action, error)

	healthMu     sync.Mutex
	healthErrors map[string]interface{}
}

type RequestLimit struct {
	Key       func(*http.Request) string
	Burst     int
	PerSecond int
}

func maxBytes(h http.Handler) http.Handler {
	const maxReqSize = 1e6 // 1MB
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// A block can easily be bigger than maxReqSize, but everything
		// else should be pretty small.
		if req.URL.Path != networkRPCPrefix+"signer/sign-block" {
			req.Body = http.MaxBytesReader(w, req.Body, maxReqSize)
		}
		h.ServeHTTP(w, req)
	})
}

func (h *Handler) init() {
	m := http.NewServeMux()
	m.Handle("/", alwaysError(errNotFound))

	latencyHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if l := latency(m, req); l != nil {
			defer l.RecordSince(time.Now())
		}
		m.ServeHTTP(w, req)
	})

	var handler = (&apiAuthn{
		tokens:   h.AccessTokens,
		tokenMap: make(map[string]tokenResult),
		alt:      h.AltAuth,
	}).handler(latencyHandler)
	handler = maxBytes(handler)
	handler = webAssetsHandler(handler)
	handler = healthHandler(handler)
	for _, l := range h.RequestLimits {
		handler = limit.Handler(handler, alwaysError(errRateLimited), l.PerSecond, l.Burst, l.Key)
	}
	handler = gzip.Handler{Handler: handler}
	handler = coreCounter(handler)
	handler = reqid.Handler(handler)
	handler = timeoutContextHandler(handler)
	h.handler = handler
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.once.Do(h.init)

	h.handler.ServeHTTP(w, r)
}

// timeoutContextHandler propagates the timeout, if any, provided as a header
// in the http request.
func timeoutContextHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		timeout, err := time.ParseDuration(req.Header.Get(rpc.HeaderTimeout))
		if err != nil {
			handler.ServeHTTP(w, req) // unmodified
			return
		}

		ctx := req.Context()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		handler.ServeHTTP(w, req.WithContext(ctx))
	})
}

func webAssetsHandler(next http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard/", static.Handler{
		Assets:  dashboard.Files,
		Default: "index.html",
	}))
	mux.Handle("/docs/", http.StripPrefix("/docs/", static.Handler{
		Assets: docs.Files,
		Index:  "index.html",
	}))
	mux.Handle("/", next)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/" {
			http.Redirect(w, req, "/dashboard/", http.StatusFound)
			return
		}

		mux.ServeHTTP(w, req)
	})
}

func (h *Handler) leaderSignHandler(f func(context.Context, *bc.Block) ([]byte, error)) func(context.Context, *bc.Block) ([]byte, error) {
	return func(ctx context.Context, b *bc.Block) ([]byte, error) {
		if f == nil {
			return nil, errNotFound // TODO(kr): is this really the right error here?
		}
		if leader.IsLeading() {
			return f(ctx, b)
		}
		var resp []byte
		err := h.forwardToLeader(ctx, "/rpc/signer/sign-block", b, &resp)
		return resp, err
	}
}

func leaderConn(ctx context.Context, db pg.DB, self string) (*grpc.ClientConn, error) {
	addr, err := leader.Address(ctx, db)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	// Don't infinite loop if the leader's address is our own address.
	// This is possible if we just became the leader. The client should
	// just retry.
	if addr == self {
		return nil, errLeaderElection
	}

	conn, err := rpc.NewGRPCConn(addr, "")
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// forwardToLeader forwards the current request to the core's leader
// process. It propagates the same credentials used in the current
// request. For that reason, it cannot be used outside of a request-
// handling context.
func (h *Handler) forwardToLeader(ctx context.Context, path string, body interface{}, resp interface{}) error {
	addr, err := leader.Address(ctx, h.DB)
	if err != nil {
		return errors.Wrap(err)
	}

	// Don't infinite loop if the leader's address is our own address.
	// This is possible if we just became the leader. The client should
	// just retry.
	if addr == h.Addr {
		return errLeaderElection
	}

	// TODO(jackson): If using TLS, use https:// here.
	l := &rpc.Client{
		BaseURL: "http://" + addr,
	}

	// Forward the request credentials if we have them.
	// TODO(jackson): Don't use the incoming request's credentials and
	// have an alternative authentication scheme between processes of the
	// same Core. For now, we only call the leader for the purpose of
	// forwarding a request, so this is OK.
	req := httpjson.Request(ctx)
	user, pass, ok := req.BasicAuth()
	if ok {
		l.AccessToken = fmt.Sprintf("%s:%s", user, pass)
	}

	return l.Call(ctx, path, body, &resp)
}

func healthHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/health" {
			return
		}
		handler.ServeHTTP(w, req)
	})
}
