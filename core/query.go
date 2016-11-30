package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"chain/core/pb"
	"chain/core/query"
	"chain/core/query/filter"
	"chain/errors"
	"chain/net/http/httpjson"
)

// These types enforce the ordering of JSON fields in API output.
type (
	txinResp struct {
		Type            interface{} `json:"type"`
		AssetID         interface{} `json:"asset_id"`
		AssetAlias      interface{} `json:"asset_alias,omitempty"`
		AssetDefinition interface{} `json:"asset_definition"`
		AssetTags       interface{} `json:"asset_tags,omitempty"`
		AssetIsLocal    interface{} `json:"asset_is_local"`
		Amount          interface{} `json:"amount"`
		IssuanceProgram interface{} `json:"issuance_program,omitempty"`
		SpentOutput     interface{} `json:"spent_output,omitempty"`
		*txAccount
		ReferenceData interface{} `json:"reference_data"`
		IsLocal       interface{} `json:"is_local"`
	}
	txoutResp struct {
		Type            interface{} `json:"type"`
		Purpose         interface{} `json:"purpose,omitempty"`
		Position        interface{} `json:"position"`
		AssetID         interface{} `json:"asset_id"`
		AssetAlias      interface{} `json:"asset_alias,omitempty"`
		AssetDefinition interface{} `json:"asset_definition"`
		AssetTags       interface{} `json:"asset_tags"`
		AssetIsLocal    interface{} `json:"asset_is_local"`
		Amount          interface{} `json:"amount"`
		*txAccount
		ControlProgram interface{} `json:"control_program"`
		ReferenceData  interface{} `json:"reference_data"`
		IsLocal        interface{} `json:"is_local"`
	}
	txResp struct {
		ID            interface{} `json:"id"`
		Timestamp     interface{} `json:"timestamp"`
		BlockID       interface{} `json:"block_id"`
		BlockHeight   interface{} `json:"block_height"`
		Position      interface{} `json:"position"`
		ReferenceData interface{} `json:"reference_data"`
		IsLocal       interface{} `json:"is_local"`
		Inputs        interface{} `json:"inputs"`
		Outputs       interface{} `json:"outputs"`
	}
	txAccount struct {
		AccountID    interface{} `json:"account_id"`
		AccountAlias interface{} `json:"account_alias,omitempty"`
		AccountTags  interface{} `json:"account_tags"`
	}
	utxoResp struct {
		Type            interface{} `json:"type"`
		Purpose         interface{} `json:"purpose"`
		TransactionID   interface{} `json:"transaction_id"`
		Position        interface{} `json:"position"`
		AssetID         interface{} `json:"asset_id"`
		AssetAlias      interface{} `json:"asset_alias"`
		AssetDefinition interface{} `json:"asset_definition"`
		AssetTags       interface{} `json:"asset_tags"`
		AssetIsLocal    interface{} `json:"asset_is_local"`
		Amount          interface{} `json:"amount"`
		AccountID       interface{} `json:"account_id"`
		AccountAlias    interface{} `json:"account_alias"`
		AccountTags     interface{} `json:"account_tags"`
		ControlProgram  interface{} `json:"control_program"`
		ReferenceData   interface{} `json:"reference_data"`
		IsLocal         interface{} `json:"is_local"`
	}
)

func protoParams(params []*pb.FilterParam) []interface{} {
	a := make([]interface{}, len(params))
	for i, p := range params {
		switch p.GetValue().(type) {
		case *pb.FilterParam_String_:
			a[i] = p.GetString_()
		case *pb.FilterParam_Int64:
			a[i] = p.GetInt64()
		case *pb.FilterParam_Bytes:
			a[i] = p.GetBytes()
		case *pb.FilterParam_Bool:
			a[i] = p.GetBool()
		}
	}
	return a
}

// ListAccounts is an http handler for listing accounts matching
// an index or an ad-hoc filter.
func (h *Handler) ListAccounts(ctx context.Context, in *pb.ListAccountsQuery) (*pb.ListAccountsResponse, error) {
	limit := defGenericPageSize

	// Build the filter predicate.
	p, err := filter.Parse(in.Filter)
	if err != nil {
		return nil, errors.Wrap(err, "parsing acc query")
	}
	after := in.After

	// Use the filter engine for querying account tags.
	accounts, after, err := h.Indexer.Accounts(ctx, p, protoParams(in.FilterParams), after, limit)
	if err != nil {
		return nil, errors.Wrap(err, "running acc query")
	}

	result := make([]*accountResponse, 0, len(accounts))
	for _, a := range accounts {
		var orderedKeys []accountKey
		keys, ok := a["keys"].([]interface{})
		if ok {
			for _, key := range keys {
				mapKey, ok := key.(map[string]interface{})
				if !ok {
					continue
				}
				orderedKeys = append(orderedKeys, accountKey{
					RootXPub:              mapKey["root_xpub"],
					AccountXPub:           mapKey["account_xpub"],
					AccountDerivationPath: mapKey["account_derivation_path"],
				})
			}
		}
		r := &accountResponse{
			ID:     a["id"],
			Alias:  a["alias"],
			Keys:   orderedKeys,
			Quorum: a["quorum"],
			Tags:   a["tags"],
		}
		result = append(result, r)
	}

	data, err := json.Marshal(httpjson.Array(result))
	if err != nil {
		return nil, errors.Wrap(err)
	}

	// Pull in the accounts by the IDs
	out := in
	out.After = after
	return &pb.ListAccountsResponse{
		Items:    data,
		LastPage: len(result) < limit,
		Next:     out,
	}, nil
}

// ListAssets is an http handler for listing assets matching
// an index or an ad-hoc filter.
func (h *Handler) ListAssets(ctx context.Context, in *pb.ListAssetsQuery) (*pb.ListAssetsResponse, error) {
	limit := defGenericPageSize

	// Build the filter predicate.
	p, err := filter.Parse(in.Filter)
	if err != nil {
		return nil, err
	}
	after := in.After

	// Use the query engine for querying asset tags.
	var assets []map[string]interface{}
	assets, after, err = h.Indexer.Assets(ctx, p, protoParams(in.FilterParams), after, limit)
	if err != nil {
		return nil, errors.Wrap(err, "running asset query")
	}

	result := make([]*assetResponse, 0, len(assets))
	for _, a := range assets {
		var orderedKeys []assetKey
		keys, ok := a["keys"].([]interface{})
		if ok {
			for _, key := range keys {
				mapKey, ok := key.(map[string]interface{})
				if !ok {
					continue
				}
				orderedKeys = append(orderedKeys, assetKey{
					AssetPubkey:         mapKey["asset_pubkey"],
					RootXPub:            mapKey["root_xpub"],
					AssetDerivationPath: mapKey["asset_derivation_path"],
				})
			}
		}
		r := &assetResponse{
			ID:              a["id"],
			IssuanceProgram: a["issuance_program"],
			Keys:            orderedKeys,
			Quorum:          a["quorum"],
			Definition:      a["definition"],
			Tags:            a["tags"],
			IsLocal:         a["is_local"],
		}
		if alias, ok := a["alias"].(string); ok && alias != "" {
			r.Alias = &alias
		}
		result = append(result, r)
	}

	data, err := json.Marshal(httpjson.Array(result))
	if err != nil {
		return nil, errors.Wrap(err)
	}

	out := in
	out.After = after
	return &pb.ListAssetsResponse{
		Items:    data,
		LastPage: len(result) < limit,
		Next:     out,
	}, nil
}

func (h *Handler) ListBalances(ctx context.Context, in *pb.ListBalancesQuery) (*pb.ListBalancesResponse, error) {
	var sumBy []filter.Field
	p, err := filter.Parse(in.Filter)
	if err != nil {
		return nil, err
	}

	// Since an empty SumBy yields a meaningless result, we'll provide a
	// sensible default here.
	if len(in.SumBy) == 0 {
		in.SumBy = []string{"asset_alias", "asset_id"}
	}

	for _, field := range in.SumBy {
		f, err := filter.ParseField(field)
		if err != nil {
			return nil, err
		}
		sumBy = append(sumBy, f)
	}

	timestampMS := in.Timestamp
	if timestampMS == 0 {
		timestampMS = math.MaxInt64
	} else if timestampMS > math.MaxInt64 {
		return nil, errors.WithDetail(httpjson.ErrBadRequest, "timestamp is too large")
	}

	// TODO(jackson): paginate this endpoint.
	balances, err := h.Indexer.Balances(ctx, p, protoParams(in.FilterParams), sumBy, timestampMS)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(httpjson.Array(balances))
	if err != nil {
		return nil, errors.Wrap(err)
	}

	return &pb.ListBalancesResponse{
		Items:    data,
		LastPage: true,
		Next:     in,
	}, nil
}

// ListTxFeeds is an http handler for listing txfeeds. It does not take a filter.
func (h *Handler) ListTxFeeds(ctx context.Context, in *pb.ListTxFeedsQuery) (*pb.ListTxFeedsResponse, error) {
	limit := defGenericPageSize
	after := in.After

	txfeeds, after, err := h.Indexer.TxFeeds(ctx, after, limit)
	if err != nil {
		return nil, errors.Wrap(err, "running txfeed query")
	}

	var pbFeeds []*pb.TxFeed
	for _, f := range txfeeds {
		proto := &pb.TxFeed{
			Id:     f.ID,
			Filter: f.Filter,
			After:  f.After,
		}
		if f.Alias != nil {
			proto.Alias = *f.Alias
		}
		pbFeeds = append(pbFeeds, proto)
	}

	out := in
	out.After = after
	return &pb.ListTxFeedsResponse{
		Items:    pbFeeds,
		LastPage: len(txfeeds) < limit,
		Next:     out,
	}, nil
}

// ListTxs is an http handler for listing transactions matching
// an index or an ad-hoc filter.
func (h *Handler) ListTxs(ctx context.Context, in *pb.ListTxsQuery) (*pb.ListTxsResponse, error) {
	var (
		timeout time.Duration
		err     error
	)

	if in.Timeout != "" {
		timeout, err = time.ParseDuration(in.Timeout)
	}
	if err != nil {
		return nil, errors.Wrap(err)
	}

	if timeout != 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, timeout)
		defer c()
	}

	// Build the filter predicate.
	p, err := filter.Parse(in.Filter)
	if err != nil {
		return nil, err
	}

	endTimeMS := in.EndTime
	if endTimeMS == 0 {
		endTimeMS = math.MaxInt64
	} else if endTimeMS > math.MaxInt64 {
		return nil, errors.WithDetail(httpjson.ErrBadRequest, "end timestamp is too large")
	}

	var after query.TxAfter
	// Either parse the provided `after` or look one up for the time range.
	if in.After != "" {
		after, err = query.DecodeTxAfter(in.After)
		if err != nil {
			return nil, errors.Wrap(err, "decoding `after`")
		}
	} else {
		after, err = h.Indexer.LookupTxAfter(ctx, in.StartTime, endTimeMS)
		if err != nil {
			return nil, err
		}
	}

	limit := defGenericPageSize
	txns, nextAfter, err := h.Indexer.Transactions(ctx, p, protoParams(in.FilterParams), after, limit, in.AscendingWithLongPoll)
	if err != nil {
		return nil, errors.Wrap(err, "running tx query")
	}

	resp := make([]*txResp, 0, len(txns))
	for _, t := range txns {
		tjson, ok := t.(*json.RawMessage)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T in Indexer.Transactions output", t)
		}
		if tjson == nil {
			return nil, fmt.Errorf("unexpected nil in Indexer.Transactions output")
		}
		var tx map[string]interface{}
		err = json.Unmarshal(*tjson, &tx)
		if err != nil {
			return nil, errors.Wrap(err, "decoding Indexer.Transactions output")
		}

		inp, ok := tx["inputs"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected type %T for inputs in Indexer.Transactions output", tx["inputs"])
		}

		var inputs []map[string]interface{}
		for i, in := range inp {
			input, ok := in.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected type %T for input %d in Indexer.Transactions output", in, i)
			}
			inputs = append(inputs, input)
		}

		outp, ok := tx["outputs"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected type %T for outputs in Indexer.Transactions output", tx["outputs"])
		}

		var outputs []map[string]interface{}
		for i, out := range outp {
			output, ok := out.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected type %T for output %d in Indexer.Transactions output", out, i)
			}
			outputs = append(outputs, output)
		}

		inResps := make([]*txinResp, 0, len(inputs))
		for _, in := range inputs {
			r := &txinResp{
				Type:            in["type"],
				AssetID:         in["asset_id"],
				AssetAlias:      in["asset_alias"],
				AssetDefinition: in["asset_definition"],
				AssetTags:       in["asset_tags"],
				AssetIsLocal:    in["asset_is_local"],
				Amount:          in["amount"],
				IssuanceProgram: in["issuance_program"],
				SpentOutput:     in["spent_output"],
				txAccount:       txAccountFromMap(in),
				ReferenceData:   in["reference_data"],
				IsLocal:         in["is_local"],
			}
			inResps = append(inResps, r)
		}
		outResps := make([]*txoutResp, 0, len(outputs))
		for _, out := range outputs {
			r := &txoutResp{
				Type:            out["type"],
				Purpose:         out["purpose"],
				Position:        out["position"],
				AssetID:         out["asset_id"],
				AssetAlias:      out["asset_alias"],
				AssetDefinition: out["asset_definition"],
				AssetTags:       out["asset_tags"],
				AssetIsLocal:    out["asset_is_local"],
				Amount:          out["amount"],
				txAccount:       txAccountFromMap(out),
				ControlProgram:  out["control_program"],
				ReferenceData:   out["reference_data"],
				IsLocal:         out["is_local"],
			}
			outResps = append(outResps, r)
		}
		r := &txResp{
			ID:            tx["id"],
			Timestamp:     tx["timestamp"],
			BlockID:       tx["block_id"],
			BlockHeight:   tx["block_height"],
			Position:      tx["position"],
			ReferenceData: tx["reference_data"],
			IsLocal:       tx["is_local"],
			Inputs:        inResps,
			Outputs:       outResps,
		}
		resp = append(resp, r)
	}

	data, err := json.Marshal(httpjson.Array(resp))
	if err != nil {
		return nil, errors.Wrap(err)
	}

	out := in
	out.After = nextAfter.String()
	return &pb.ListTxsResponse{
		Items:    data,
		LastPage: len(resp) < limit,
		Next:     out,
	}, nil
}

func (h *Handler) ListUnspentOutputs(ctx context.Context, in *pb.ListUnspentOutputsQuery) (*pb.ListUnspentOutputsResponse, error) {
	p, err := filter.Parse(in.Filter)
	if err != nil {
		return nil, err
	}

	var after *query.OutputsAfter
	if in.After != "" {
		after, err = query.DecodeOutputsAfter(in.After)
		if err != nil {
			return nil, errors.Wrap(err, "decoding `after`")
		}
	}

	timestampMS := in.Timestamp
	if timestampMS == 0 {
		timestampMS = math.MaxInt64
	} else if timestampMS > math.MaxInt64 {
		return nil, errors.WithDetail(httpjson.ErrBadRequest, "timestamp is too large")
	}
	limit := defGenericPageSize
	outputs, nextAfter, err := h.Indexer.Outputs(ctx, p, protoParams(in.FilterParams), timestampMS, after, limit)
	if err != nil {
		return nil, errors.Wrap(err, "querying outputs")
	}

	resp := make([]*utxoResp, 0, len(outputs))
	for _, o := range outputs {
		ojson, ok := o.(*json.RawMessage)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T in Indexer.Outputs output", o)
		}
		if ojson == nil {
			return nil, fmt.Errorf("unexpected nil in Indexer.Outputs output")
		}
		var out map[string]interface{}
		err = json.Unmarshal(*ojson, &out)
		if err != nil {
			return nil, errors.Wrap(err, "decoding Indexer.Outputs output")
		}
		r := &utxoResp{
			Type:            out["type"],
			Purpose:         out["purpose"],
			TransactionID:   out["transaction_id"],
			Position:        out["position"],
			AssetID:         out["asset_id"],
			AssetAlias:      out["asset_alias"],
			AssetDefinition: out["asset_definition"],
			AssetTags:       out["asset_tags"],
			AssetIsLocal:    out["asset_is_local"],
			Amount:          out["amount"],
			AccountID:       out["account_id"],
			AccountAlias:    out["account_alias"],
			AccountTags:     out["account_tags"],
			ControlProgram:  out["control_program"],
			ReferenceData:   out["reference_data"],
			IsLocal:         out["is_local"],
		}
		resp = append(resp, r)
	}

	data, err := json.Marshal(httpjson.Array(resp))
	if err != nil {
		return nil, errors.Wrap(err)
	}

	outQuery := in
	outQuery.After = nextAfter.String()
	return &pb.ListUnspentOutputsResponse{
		Items:    data,
		LastPage: len(resp) < limit,
		Next:     outQuery,
	}, nil
}

func txAccountFromMap(m map[string]interface{}) *txAccount {
	if _, ok := m["account_id"]; !ok {
		return nil
	}
	return &txAccount{
		AccountID:    m["account_id"],
		AccountAlias: m["account_alias"],
		AccountTags:  m["account_tags"],
	}
}
