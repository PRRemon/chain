package core

import (
	"context"
	"sync"

	"chain/core/pb"
)

// POST /create-control-program
func (h *Handler) CreateControlPrograms(ctx context.Context, in *pb.CreateControlProgramsRequest) (*pb.CreateControlProgramsResponse, error) {
	responses := make([]*pb.CreateControlProgramsResponse_Response, len(in.Requests))
	var wg sync.WaitGroup
	wg.Add(len(responses))

	for i := 0; i < len(responses); i++ {
		go func(i int) {
			defer wg.Done()
			switch in.Requests[i].GetType().(type) {
			case (*pb.CreateControlProgramsRequest_Request_Account):
				responses[i] = h.createAccountControlProgram(ctx, in.Requests[i].GetAccount())
			}
		}(i)
	}

	wg.Wait()
	return &pb.CreateControlProgramsResponse{Responses: responses}, nil
}

func (h *Handler) createAccountControlProgram(ctx context.Context, in *pb.CreateControlProgramsRequest_Account) *pb.CreateControlProgramsResponse_Response {
	resp := new(pb.CreateControlProgramsResponse_Response)

	accountID := in.GetAccountId()
	if accountID == "" {
		acc, err := h.Accounts.FindByAlias(ctx, in.GetAccountAlias())
		if err != nil {
			detailedErr, _ := errInfo(err)
			resp.Error = protobufErr(detailedErr)
			return resp
		}
		accountID = acc.ID
	}

	controlProgram, err := h.Accounts.CreateControlProgram(ctx, accountID, false)
	if err != nil {
		detailedErr, _ := errInfo(err)
		resp.Error = protobufErr(detailedErr)
		return resp
	}

	resp.ControlProgram = controlProgram
	return resp
}
