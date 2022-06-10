package batchRequestService

import (
	"context"
	"errors"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/samber/lo"
)

type BatchRequestService struct {
	client jsonrpc.RPCClient
}

func New(endPoint string) *BatchRequestService {
	return &BatchRequestService{
		client: jsonrpc.NewClient(endPoint),
	}
}

type BatchRequester interface {
	GetSignaturesForAddresses(accounts []solana.PublicKey) (map[solana.PublicKey][]*rpc.TransactionSignature, error)
	GetTransactions(signatures []solana.Signature) (map[solana.Signature]*rpc.TransactionWithMeta, error)
}

func (b *BatchRequestService) GetSignaturesForAddresses(accounts []solana.PublicKey) (map[solana.PublicKey][]*rpc.TransactionSignature, error) {
	outMap := make(map[solana.PublicKey][]*rpc.TransactionSignature)
	batch, err := b.client.CallBatch(context.TODO(), lo.Map(accounts, func(t solana.PublicKey, i int) *jsonrpc.RPCRequest {
		return jsonrpc.NewRequest("getSignaturesForAddress", []interface{}{t, rpc.M{"commitment": rpc.CommitmentConfirmed, "limit": 15}})
	}),
	)
	if err != nil {
		return outMap, err
	}
	lo.ForEach(batch, func(t *jsonrpc.RPCResponse, i int) {
		var out []*rpc.TransactionSignature
		err := t.GetObject(&out)
		if err != nil {
			panic(err)
		}
		outMap[accounts[t.ID]] = out
	})
	return outMap, nil
}

func (b *BatchRequestService) GetTransactions(signatures []solana.Signature) (map[solana.Signature]*rpc.TransactionWithMeta, error) {

	outMap := make(map[solana.Signature]*rpc.TransactionWithMeta)
	batch, err := b.client.CallBatch(context.TODO(), lo.Map(signatures, func(t solana.Signature, i int) *jsonrpc.RPCRequest {
		return jsonrpc.NewRequest("getConfirmedTransaction", []interface{}{t, rpc.M{"commitment": rpc.CommitmentConfirmed}})
	}),
	)
	if err != nil {
		return outMap, err
	}
	if len(signatures) != len(batch) {
		return outMap, errors.New(fmt.Sprintf("Batch input/output mismatch %v vs %v", len(signatures), len(batch)))
	}
	lo.ForEach(batch, func(t *jsonrpc.RPCResponse, i int) {
		var out *rpc.TransactionWithMeta
		err := t.GetObject(&out)
		if err != nil {
			return
		}
		outMap[signatures[t.ID]] = out
	})
	return outMap, nil
}

type GetTransaction2Params struct {
	commitment string
}

func (b *BatchRequestService) GetTransactions2(signatures []solana.Signature, params GetTransaction2Params) (map[solana.Signature]*rpc.GetTransactionResult, error) {
	outMap := make(map[solana.Signature]*rpc.GetTransactionResult)
	batch, err := b.client.CallBatch(context.TODO(), lo.Map(signatures, func(t solana.Signature, i int) *jsonrpc.RPCRequest {
		return jsonrpc.NewRequest(
			"getTransaction",
			[]interface{}{
				t,
				rpc.M{"commitment": params.commitment},
			},
		)
	}),
	)
	if err != nil {
		return outMap, err
	}
	if len(signatures) != len(batch) {
		return outMap, errors.New(fmt.Sprintf("Batch input/output mismatch %v vs %v", len(signatures), len(batch)))
	}
	lo.ForEach(batch, func(t *jsonrpc.RPCResponse, i int) {
		var out *rpc.GetTransactionResult
		err := t.GetObject(&out)
		if err != nil {
			return
		}
		outMap[signatures[t.ID]] = out
	})
	return outMap, nil
}
