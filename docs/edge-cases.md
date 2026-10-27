# Edge cases

Every failure mode considered, how it is handled, and the test that covers it.

**Status key:**
- ✅ handled and tested
- ☑️ handled by design, without a dedicated test
- ⚠️ known limitation
- ➡️ the consumer's responsibility

## Chain behaviour

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 1 | Shallow reorg replaces the latest blocks | ✅ | The first new block's `parentHash` is compared with the cursor. On a mismatch the indexer walks back to the common ancestor and rewinds in one transaction. Orphaned transfers move to `orphaned_transfers`. | `TestReorgReplacesOrphanedTransfers` |
| 2 | Reorg at the same height (no new blocks) | ✅ | When idle, the cursor's hash is re-checked against the node. | `TestDetectsReorgWithoutNewBlocks` |
| 3 | Reorg that makes the chain shorter | ✅ | This looks the same as a node that is behind, so the indexer waits. The rewind happens once a block at the cursor height exists and its hash proves the fork. | `TestDetectsReorgWithoutNewBlocks` |
| 4 | Reorg in the middle of a fetch (between headers and logs) | ✅ | Every log's `blockHash` must match its header, and the batch tip is re-read after the logs. A mismatch discards the batch and retries it. | `TestSurvivesReorgDuringFetch` |
| 5 | A transaction re-mined in a different block after a reorg | ✅ | The old row is orphaned and the new one inserted. Events carry the block hash, so `confirmed(A)`, `reverted(A)` and `confirmed(B)` net to one credit. | `TestReorgReplacesOrphanedTransfers`, randomised simulation |
| 6 | A transaction dropped entirely by a reorg | ✅ | Orphaned. If it had been announced, a `transfer.reverted` event is emitted. | `TestReorgReplacesOrphanedTransfers` |
| 7 | The node disagrees with blocks already treated as final | ✅ | The indexer refuses to rewind below the finalized height and logs an error. Data is never deleted. | `TestRefusesToRewindFinalizedData` |
| 8 | Crediting before finality | ✅ | `CONFIRM_AT` defaults to `finalized`. At `pending` or `safe`, a later reorg emits `transfer.reverted`. | `TestConfirmsOnceAtTheConfiguredLevel` |
| 9 | The `safe` tag moves backwards (an L1 reorg before L1 finality) | ⚠️ | Rows already marked `safe` stay `safe` until finalized. The default `CONFIRM_AT=finalized` is unaffected. | — |
| 10 | A reorg deeper than the unfinalized window | ☑️ | Impossible by definition on the OP Stack. Treated as #7. | — |

## Duplicates and idempotency

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 11 | The same log returned twice (overlapping queries, a transfer between two watched addresses) | ✅ | Logs are merged in memory, and the database enforces `UNIQUE (chain_id, tx_hash, log_index)` with `ON CONFLICT DO NOTHING`. | `TestIndexesEachWatchedTransferOnce`, `TestTransfersQueriesBothDirectionsAndMergesDuplicates` |
| 12 | Restart after a crash | ✅ | The cursor moves only in the batch's own transaction, so a restart resumes exactly where it stopped. | `TestIndexesEachWatchedTransferOnce`, randomised simulation |
| 13 | Commit succeeded but the acknowledgement was lost, so the step is retried | ✅ | The next step reads the cursor again. A stale batch is rejected with `ErrCursorMoved`. | `TestRejectsStaleBatches` |
| 14 | Two indexer instances writing at the same time | ✅ | An advisory transaction lock plus an optimistic cursor check: the loser gets `ErrCursorMoved` and retries. | `TestConcurrentIndexersNeverDuplicate` |
| 15 | Crash in the middle of a batch | ☑️ | One PostgreSQL transaction: either everything commits or nothing does. | — |
| 16 | A transfer reaches the confirmation level in the same batch it was inserted | ✅ | Confirmations are emitted after the insert, in the same transaction. | `TestIndexesEachWatchedTransferOnce` |
| 17 | The outbox relay crashes after publishing but before marking events sent | ➡️ | Delivery is at-least-once. `eventId` is deterministic and consumers deduplicate on it. SQS FIFO drops resends within 5 minutes, and [usdc-deposit-ledger](https://github.com/BrianLiu0531/usdc-deposit-ledger) uses a DynamoDB conditional write after that. | `TestPublishesEachEventOnceInOrder`, `TestSQSPublisherBatchesGroupsAndDeduplicates`, ledger's `TestConcurrentDuplicatesApplyOnce` |
| 18 | The message bus is down | ✅ | Events stay pending and are retried with backoff. | `TestPublishesEachEventOnceInOrder` |
| 19 | Several relays at once | ☑️ | `FOR UPDATE SKIP LOCKED` prevents double sends, but cross-relay ordering is not guaranteed. Run one relay, or use a FIFO queue grouped per transfer. | — |

## Token-level cases

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 20 | Fake "USDC" tokens (scam contracts emitting `Transfer`) | ✅ | Logs are filtered by the official contract address per network. | `TestTransfersQueriesBothDirectionsAndMergesDuplicates` |
| 21 | Zero-value transfers (address-poisoning spam) | ✅ | Stored for the audit trail, never announced as deposits. | `TestZeroValueTransfersAreStoredButNotAnnounced` |
| 22 | Self-transfers | ✅ | Recorded once, with direction `self`. Net flow is 0. | `TestTransfersArePagedNewestFirst` |
| 23 | Mint (`from = 0x0`) and burn (`to = 0x0`) | ☑️ | Standard `Transfer` events, handled like any other. Reconciliation stays consistent. | — |
| 24 | Values up to 2²⁵⁶ | ☑️ | Stored as `NUMERIC(78)`, using big integers end to end, never floats. | — |
| 25 | Malformed logs (wrong topic count or data length) | ☑️ | Skipped during decoding. | — |
| 26 | USDC proxy upgrade, pause or blacklist | ☑️ | The address and event signature don't change. A paused or blacklisted address simply has no transfers. | — |

## RPC and infrastructure

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 27 | RPC timeouts, 5xx errors, an endpoint down | ✅ | Retries with exponential backoff and jitter, rotating through endpoints, then sticking with the one that works. | `TestFailsOverToTheNextEndpoint`, randomised simulation (10–30% failures) |
| 28 | Rate limits (429), with batch elements counted individually | ✅ | A client-side token bucket charges one token per batch element. Verified against the public Base RPC's 25 requests/s limit. | live run |
| 29 | `eth_getLogs` range or result limits | ✅ | The indexer halves the range, learns the provider's ceiling and grows again after successes. | `TestShrinksTheLogRangeWhenRejected`, `TestIsRangeError` |
| 30 | A node behind a load balancer answers logs past its own head (silently incomplete) | ✅ | The node that serves `eth_getLogs` must report a head at or above the range end. | `TestIgnoresLogsFromANodeBehindTheRange` |
| 31 | Failover to a lagging node | ✅ | The indexer waits rather than rewinding. | `TestWaitsForALaggingNode` |
| 32 | `NOT FOUND` from an inconsistent node during the reorg walk | ✅ | Treated as transient and retried, never as evidence of a reorg. | `TestWaitsForALaggingNode` |
| 33 | RPC pointing at the wrong chain | ☑️ | The chain ID is checked at start-up, and all data is keyed by chain ID. | — |
| 34 | An API key in the RPC URL leaking into logs | ✅ | URLs are reduced to scheme and host, and errors are scrubbed. | `TestErrorsDoNotLeakAPIKeys` |
| 35 | Database unavailable | ☑️ | The step fails and is retried with backoff. No partial writes. | — |
| 36 | Graceful shutdown | ☑️ | `SIGTERM` cancels the context, and the HTTP server drains for up to 10 seconds. | — |

## Reconciliation

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 37 | A missed or extra transfer anywhere in history | ✅ | `balanceOf(address, finalized)` must equal the opening balance plus indexed net flow. Any difference sets a metric and logs an error. | `TestReconcilesAgainstBalanceOf` |
| 38 | Reconciliation while the indexer is behind | ✅ | Reconciliation only runs at the indexer's own finalized height. | `TestReconcilesAgainstBalanceOf` |
| 39 | The node has no historical state for `balanceOf` (not an archive node) | ⚠️ | The check fails with a warning and indexing is unaffected. It runs at a recent height, which full nodes normally keep. | — |

## Configuration

| # | Case | Status | How it is handled | Test |
|---|---|---|---|---|
| 40 | An address added to `WATCH_ADDRESSES` later | ⚠️ | Only indexed from the current cursor onwards, so its history is missing. Re-index from `START_BLOCK`, or wait for per-address start blocks (planned). | — |
| 41 | `CONFIRM_AT` changed between runs | ☑️ | Transfers already announced stay announced, and newly eligible ones are announced once. | — |
| 42 | One block containing more watched logs than the provider's result cap | ⚠️ | The range cannot shrink below one block, so the step keeps failing. Not realistic for a watch list. | — |
