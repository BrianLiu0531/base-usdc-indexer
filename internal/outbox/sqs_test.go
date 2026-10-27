package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
)

type fakeSQS struct {
	batches [][]types.SendMessageBatchRequestEntry
	failOne bool
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.batches = append(f.batches, in.Entries)
	out := &sqs.SendMessageBatchOutput{}
	if f.failOne {
		out.Failed = []types.BatchResultErrorEntry{{Id: in.Entries[0].Id, Code: aws.String("ThrottlingException"), Message: aws.String("slow down")}}
	}
	return out, nil
}

func events(n int) []store.Event {
	out := make([]store.Event, n)
	for i := range out {
		id := fmt.Sprintf("transfer.confirmed:84532:0x%064x:%d:0x%064x", i, i%3, i) // ~160 chars, over SQS's dedup limit
		payload := fmt.Sprintf(`{"type":"transfer.confirmed","chainId":84532,"txHash":"0x%064x","logIndex":%d,"value":"5"}`, i, i%3)
		out[i] = store.Event{ID: int64(i + 1), EventID: id, Type: "transfer.confirmed", Payload: json.RawMessage(payload)}
	}
	return out
}

func TestSQSPublisherBatchesGroupsAndDeduplicates(t *testing.T) {
	f := &fakeSQS{}
	if err := (SQSPublisher{Client: f, QueueURL: "q"}).Publish(context.Background(), events(23)); err != nil {
		t.Fatal(err)
	}
	if len(f.batches) != 3 || len(f.batches[0]) != 10 || len(f.batches[2]) != 3 {
		t.Fatalf("batch sizes %d/%d", len(f.batches), len(f.batches[2]))
	}
	first := f.batches[0][1]
	if g := aws.ToString(first.MessageGroupId); g != fmt.Sprintf("84532:0x%064x:1", 1) {
		t.Fatalf("group %s", g)
	}
	if d := aws.ToString(first.MessageDeduplicationId); len(d) != 64 {
		t.Fatalf("dedup id %q must be a 64-char hash", d)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(first.MessageBody)), &body); err != nil || !strings.HasPrefix(body["eventId"].(string), "transfer.confirmed:84532:") {
		t.Fatalf("body %v, %v", body, err)
	}
}

func TestSQSPublisherFailsOnPartialRejection(t *testing.T) {
	f := &fakeSQS{failOne: true}
	err := (SQSPublisher{Client: f, QueueURL: "q"}).Publish(context.Background(), events(2))
	if err == nil || !strings.Contains(err.Error(), "ThrottlingException") {
		t.Fatalf("got %v", err)
	}
}
