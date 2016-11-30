package rpc

import (
	"errors"
	"strings"

	libcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type coreCreds struct {
	Username, Password string
}

func newRPCCreds(token string) (credentials.PerRPCCredentials, error) {
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return nil, errors.New("invalid token string")
	}
	return &coreCreds{parts[0], parts[1]}, nil
}

func (c *coreCreds) GetRequestMetadata(ctx libcontext.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}
func (c *coreCreds) RequireTransportSecurity() bool { return false }

func NewGRPCConn(addr, accesstoken string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if accesstoken != "" {
		creds, err := newRPCCreds(accesstoken)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithPerRPCCredentials(creds))
	}
	opts = append(opts, grpc.WithCompressor(grpc.NewGZIPCompressor()))
	opts = append(opts, grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	if strings.HasPrefix(addr, "localhost") || strings.HasPrefix(addr, "127.0.0.1") || strings.HasPrefix(addr, ":") {
		opts = append(opts, grpc.WithInsecure())
	}

	return grpc.Dial(addr, opts...)
}
