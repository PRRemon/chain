package core

import (
	"context"
	"encoding/json"
	"sync"

	"chain/core/pb"
	"chain/core/signers"
	"chain/net/http/reqid"
)

// This type enforces JSON field ordering in API output.
type assetResponse struct {
	ID              interface{} `json:"id"`
	Alias           *string     `json:"alias"`
	IssuanceProgram interface{} `json:"issuance_program"`
	Keys            interface{} `json:"keys"`
	Quorum          interface{} `json:"quorum"`
	Definition      interface{} `json:"definition"`
	Tags            interface{} `json:"tags"`
	IsLocal         interface{} `json:"is_local"`
}

type assetKey struct {
	RootXPub            interface{} `json:"root_xpub"`
	AssetPubkey         interface{} `json:"asset_pubkey"`
	AssetDerivationPath interface{} `json:"asset_derivation_path"`
}

// POST /create-asset
func (h *Handler) CreateAssets(ctx context.Context, in *pb.CreateAssetsRequest) (*pb.CreateAssetsResponse, error) {
	responses := make([]*pb.CreateAssetsResponse_Response, len(in.Requests))
	var wg sync.WaitGroup
	wg.Add(len(responses))

	for i := range responses {
		go func(i int) {
			subctx := reqid.NewSubContext(ctx, reqid.New())
			defer wg.Done()
			defer batchRecover(func(err error) {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
			})

			var tags, def map[string]interface{}
			err := json.Unmarshal(in.Requests[i].Tags, &tags)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}
			err = json.Unmarshal(in.Requests[i].Definition, &def)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}

			asset, err := h.Assets.Define(
				subctx,
				in.Requests[i].RootXpubs,
				int(in.Requests[i].Quorum),
				def,
				in.Requests[i].Alias,
				tags,
				in.Requests[i].ClientToken,
			)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}
			var keys []*pb.Asset_Key
			for _, xpub := range asset.Signer.XPubs {
				path := signers.Path(asset.Signer, signers.AssetKeySpace)
				derived := xpub.Derive(path)
				keys = append(keys, &pb.Asset_Key{
					AssetPubkey:         derived[:],
					RootXpub:            xpub[:],
					AssetDerivationPath: path,
				})
			}
			responses[i] = &pb.CreateAssetsResponse_Response{
				Asset: &pb.Asset{
					Id:              asset.AssetID.String(),
					IssuanceProgram: asset.IssuanceProgram,
					Keys:            keys,
					Quorum:          int32(asset.Signer.Quorum),
					Definition:      in.Requests[i].Definition,
					Tags:            in.Requests[i].Tags,
					IsLocal:         true,
				},
			}
		}(i)
	}

	wg.Wait()
	return &pb.CreateAssetsResponse{Responses: responses}, nil
}
