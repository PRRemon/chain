package txbuilder

import (
	"chain/core/pb"
	"chain/protocol/bc"
	"context"
	"time"
)

type Template struct {
	*pb.TxTemplate
	Tx        *bc.TxData
	sigHasher *bc.SigHasher
}

func (t *Template) Hash(idx uint32) bc.Hash {
	if t.sigHasher == nil {
		t.sigHasher = bc.NewSigHasher(t.Tx)
	}
	return t.sigHasher.Hash(int(idx))
}

type Action interface {
	// TODO(bobg, jeffomatic): see if there is a way to remove the maxTime
	// parameter from the build call. One possibility would be to treat TTL as
	// a transaction-wide default parameter that gets folded into actions that
	// care about it. This could happen when the build request is being
	// deserialized.
	Build(context.Context, time.Time, *TemplateBuilder) error
}
