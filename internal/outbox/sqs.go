package outbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrianLiu0531/base-usdc-indexer/internal/store"
)

// SQSAPI is the subset of the SQS client the publisher uses.
type SQSAPI interface {
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// SQSPublisher sends events to an SQS FIFO queue.
//
//   - MessageGroupId is the transfer (chain, tx hash, log index), so a transfer's confirmed and
//     reverted events are delivered in order, while different transfers are processed in parallel.
//   - MessageDeduplicationId is derived from the event ID, so SQS drops a resend within its
//     5-minute window. After that, the consumer's own idempotency check takes over.
type SQSPublisher struct {
	Client   SQSAPI
	QueueURL string
}

const sqsBatchLimit = 10

func (p SQSPublisher) Publish(ctx context.Context, events []store.Event) error {
	for start := 0; start < len(events); start += sqsBatchLimit {
		chunk := events[start:min(len(events), start+sqsBatchLimit)]
		entries := make([]types.SendMessageBatchRequestEntry, len(chunk))
		for i, e := range chunk {
			body, group, err := message(e)
			if err != nil {
				return err
			}
			dedup := sha256.Sum256([]byte(e.EventID)) // event IDs can exceed SQS's 128-character limit
			entries[i] = types.SendMessageBatchRequestEntry{
				Id:                     aws.String(strconv.Itoa(i)),
				MessageBody:            aws.String(body),
				MessageGroupId:         aws.String(group),
				MessageDeduplicationId: aws.String(hex.EncodeToString(dedup[:])),
			}
		}
		out, err := p.Client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &p.QueueURL, Entries: entries})
		if err != nil {
			return fmt.Errorf("send to SQS: %w", err)
		}
		if len(out.Failed) > 0 {
			// The whole outbox batch is retried; entries that did succeed are dropped by SQS deduplication
			reasons := make([]string, len(out.Failed))
			for i, f := range out.Failed {
				reasons[i] = aws.ToString(f.Code) + ": " + aws.ToString(f.Message)
			}
			return fmt.Errorf("SQS rejected %d of %d messages: %s", len(out.Failed), len(entries), strings.Join(reasons, "; "))
		}
	}
	return nil
}

// message builds the SQS body (the event payload plus its ID) and the FIFO group of an event.
func message(e store.Event) (body, group string, err error) {
	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(e.Payload))
	dec.UseNumber() // keep numbers exact (no float64 formatting such as 1e+06)
	if err := dec.Decode(&payload); err != nil {
		return "", "", fmt.Errorf("event %s: %w", e.EventID, err)
	}
	payload["eventId"] = e.EventID
	b, err := json.Marshal(payload)
	if err != nil {
		return "", "", err
	}
	return string(b), fmt.Sprintf("%v:%v:%v", payload["chainId"], payload["txHash"], payload["logIndex"]), nil
}
