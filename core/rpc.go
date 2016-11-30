package core

import (
	"context"
	"net/http"

	libcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"chain/core/config"
	"chain/core/leader"
	"chain/core/pb"
	"chain/core/txdb"
	"chain/database/pg"
	"chain/errors"
	"chain/net/http/limit"
	"chain/net/http/reqid"
	"chain/protocol"
	"chain/protocol/bc"
)

type rpcServer struct {
	Config       *config.Config
	Chain        *protocol.Chain
	Store        *txdb.Store
	DB           pg.DB
	RequestLimit int
	Signer       func(context.Context, *bc.Block) ([]byte, error)
	Addr         string

	auth    *apiAuthn
	limiter *limit.BucketLimiter
}

func (r *rpcServer) Handler() http.Handler {
	r.auth = &apiAuthn{
		tokenMap: make(map[string]tokenResult),
	}
	r.limiter = limit.NewBucketLimiter(r.RequestLimit, 100)

	var opts []grpc.ServerOption

	opts = append(opts, grpc.RPCCompressor(grpc.NewGZIPCompressor()))
	opts = append(opts, grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	opts = append(opts, grpc.UnaryInterceptor(r.unaryInterceptor))
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterNodeServer(grpcServer, r)
	if r.Config != nil && r.Config.IsSigner {
		pb.RegisterSignerServer(grpcServer, r)
	}

	return grpcServer
}

func (r *rpcServer) GetBlock(ctx libcontext.Context, in *pb.GetBlockRequest) (*pb.GetBlockResponse, error) {
	err := <-r.Chain.BlockSoonWaiter(ctx, in.Height)
	if err != nil {
		return nil, errors.Wrapf(err, "waiting for block at height %d", in.Height)
	}

	rawBlock, err := r.Store.GetRawBlock(ctx, in.Height)
	if err != nil {
		return nil, err
	}

	return &pb.GetBlockResponse{Block: rawBlock}, nil
}

func (r *rpcServer) GetSnapshotInfo(ctx libcontext.Context, in *pb.Empty) (*pb.GetSnapshotInfoResponse, error) {
	height, size, err := r.Store.LatestSnapshotInfo(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetSnapshotInfoResponse{
		Height:       height,
		Size:         size,
		BlockchainId: r.Config.BlockchainID[:],
	}, nil
}

func (r *rpcServer) GetSnapshot(ctx libcontext.Context, in *pb.GetSnapshotRequest) (*pb.GetSnapshotResponse, error) {
	data, err := r.Store.GetSnapshot(ctx, in.Height)
	if err != nil {
		return nil, err
	}

	return &pb.GetSnapshotResponse{Data: data}, nil
}

func (r *rpcServer) GetBlockHeight(ctx libcontext.Context, in *pb.Empty) (*pb.GetBlockHeightResponse, error) {
	return &pb.GetBlockHeightResponse{Height: r.Chain.Height()}, nil
}

func (r *rpcServer) SubmitTx(ctx libcontext.Context, in *pb.SubmitTxRequest) (*pb.SubmitTxResponse, error) {
	txdata, err := bc.NewTxDataFromBytes(in.Transaction)
	if err != nil {
		return nil, err
	}
	err = r.Chain.AddTx(ctx, bc.NewTx(*txdata))
	if err != nil {
		return nil, err
	}
	return &pb.SubmitTxResponse{Ok: true}, nil
}

func (r *rpcServer) SignBlock(ctx libcontext.Context, in *pb.SignBlockRequest) (*pb.SignBlockResponse, error) {
	if !leader.IsLeading() {
		conn, err := leaderConn(ctx, r.DB, r.Addr)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		return pb.NewSignerClient(conn).SignBlock(ctx, in)
	}
	block, err := bc.NewBlockFromBytes(in.Block)
	if err != nil {
		return nil, err
	}
	sig, err := r.Signer(ctx, block)
	if err != nil {
		return nil, err
	}
	return &pb.SignBlockResponse{Signature: sig}, nil
}

func (r *rpcServer) unaryInterceptor(ctx libcontext.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = reqid.NewContext(ctx, reqid.New())

	if err := r.limit(ctx); err != nil {
		return nil, err
	}

	ctx, err := r.auth.authRPC(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := handler(ctx, req)
	if err != nil {
		detailedErr, _ := errInfo(err)
		resp = &pb.ErrorResponse{Error: protobufErr(detailedErr)}
	}
	return resp, nil
}

func (r *rpcServer) limit(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errRateLimited
	}

	if !r.limiter.Allow(p.Addr.String()) {
		return errRateLimited
	}
	return nil
}
