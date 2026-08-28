package agent

import (
	"context"
	"sort"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"

	"github.com/wzshiming/xet-pd/segstore"
	"github.com/wzshiming/xet-pd/tracker"
)

const (
	// announceInterval is the keepalive cadence; claims carry announceTTL so
	// a peer surviving two missed announces still resolves.
	announceInterval = 5 * time.Minute
	announceTTL      = 15 * time.Minute
	// announceBatchSize bounds one announce request so a large ready index
	// never exceeds the tracker's request-body limit.
	announceBatchSize = 512
)

// announceItem is one queued notification: a freshly ready file or a
// freshly captured xorb chunk span, exactly one of the two set.
type announceItem struct {
	file *tracker.FileAdd
	xorb *tracker.XorbAdd
}

// announcer keeps the tracker's view of this agent's holdings current:
// freshly ready files and captured segments are queued and coalesced into
// batched announces by a single worker, and a periodic full re-announce over
// the ready index, the segment store and the shards of ready files acts as
// keepalive. Announce failures are dropped — a missing tracker only degrades
// discovery back to S1 probing.
type announcer struct {
	client *tracker.Client
	peer   string
	idx    readyIndex
	stor   storage.Storage
	segs   *segstore.Store

	ctx     context.Context
	cancel  context.CancelFunc
	pending chan announceItem
	stopped chan struct{}
}

func newAnnouncer(client *tracker.Client, peer string, idx readyIndex, stor storage.Storage, segs *segstore.Store) *announcer {
	ctx, cancel := context.WithCancel(context.Background())
	return &announcer{
		client:  client,
		peer:    peer,
		idx:     idx,
		stor:    stor,
		segs:    segs,
		ctx:     ctx,
		cancel:  cancel,
		pending: make(chan announceItem, 4*announceBatchSize),
		stopped: make(chan struct{}),
	}
}

// run announces everything already held, then keeps the claims alive and
// flushes queued notifications until stop.
func (an *announcer) run() {
	defer close(an.stopped)
	an.announceAll()
	ticker := time.NewTicker(announceInterval)
	defer ticker.Stop()
	for {
		select {
		case item := <-an.pending:
			an.flush(an.drain(item))
		case <-ticker.C:
			an.announceAll()
		case <-an.ctx.Done():
			return
		}
	}
}

// stop cancels in-flight announces and waits for the worker to exit.
func (an *announcer) stop() {
	an.cancel()
	<-an.stopped
}

// notify queues one freshly ready file without blocking the caller. A full
// queue drops the notification: the periodic re-announce repairs the claim.
func (an *announcer) notify(e *readyEntry, keys []string) {
	select {
	case an.pending <- announceItem{file: &tracker.FileAdd{SHA256: e.SHA256, FileHash: e.FileHash, Keys: keys}}:
	default:
	}
}

// notifySegment queues one freshly captured xorb chunk span without
// blocking the caller, dropping on a full queue like notify.
func (an *announcer) notifySegment(xorbHash string, span tracker.ChunkSpan) {
	select {
	case an.pending <- announceItem{xorb: &tracker.XorbAdd{Hash: xorbHash, Ranges: []tracker.ChunkSpan{span}}}:
	default:
	}
}

// drain coalesces whatever is already queued behind first into one batch.
func (an *announcer) drain(first announceItem) []announceItem {
	items := []announceItem{first}
	for len(items) < announceBatchSize {
		select {
		case item := <-an.pending:
			items = append(items, item)
		default:
			return items
		}
	}
	return items
}

// flush splits one drained batch into file and xorb adds, merging xorb
// spans per hash so a burst of captures for one xorb costs one entry.
func (an *announcer) flush(items []announceItem) {
	var files []tracker.FileAdd
	spans := map[string][]tracker.ChunkSpan{}
	for _, item := range items {
		switch {
		case item.file != nil:
			files = append(files, *item.file)
		case item.xorb != nil:
			spans[item.xorb.Hash] = append(spans[item.xorb.Hash], item.xorb.Ranges...)
		}
	}
	an.announce(files, mergedXorbAdds(spans))
}

// mergedXorbAdds turns per-hash span lists into sorted, merged announce
// entries.
func mergedXorbAdds(spans map[string][]tracker.ChunkSpan) []tracker.XorbAdd {
	if len(spans) == 0 {
		return nil
	}
	adds := make([]tracker.XorbAdd, 0, len(spans))
	for hash, ss := range spans {
		adds = append(adds, tracker.XorbAdd{Hash: hash, Ranges: tracker.MergeSpans(ss)})
	}
	sort.Slice(adds, func(i, j int) bool { return adds[i].Hash < adds[j].Hash })
	return adds
}

// announceAll walks the ready index and re-announces every entry in bounded
// batches, grouping resolve-path aliases of the same content into one add,
// plus every held xorb chunk range: stored segments and the full xorbs
// named by the shards of ready files.
func (an *announcer) announceAll() {
	var files []tracker.FileAdd
	bySHA := map[string]int{}
	an.idx.walk(func(e *readyEntry) {
		if i, ok := bySHA[e.SHA256]; ok {
			files[i].Keys = append(files[i].Keys, e.Key)
			return
		}
		bySHA[e.SHA256] = len(files)
		files = append(files, tracker.FileAdd{SHA256: e.SHA256, FileHash: e.FileHash, Keys: []string{e.Key}})
	})
	xorbs := an.collectXorbs()
	for len(files) > 0 || len(xorbs) > 0 {
		nf := min(len(files), announceBatchSize)
		nx := min(len(xorbs), announceBatchSize-nf)
		an.announce(files[:nf], xorbs[:nx])
		files, xorbs = files[nf:], xorbs[nx:]
	}
}

// collectXorbs gathers every chunk span this agent can serve: each stored
// segment's span, and the full range of every xorb in the shard of each
// distinct ready file. Unreadable shards degrade to the segment claims —
// announceAll never fails.
func (an *announcer) collectXorbs() []tracker.XorbAdd {
	spans := map[string][]tracker.ChunkSpan{}
	if an.segs != nil {
		_ = an.segs.Walk(func(seg segstore.Segment) error {
			// Quarantined segments are unverified: never announced.
			if seg.Quarantined {
				return nil
			}
			spans[seg.XorbHash] = append(spans[seg.XorbHash], tracker.ChunkSpan{Start: seg.ChunkStart, End: seg.ChunkEnd})
			return nil
		})
	}
	if an.stor != nil {
		seen := map[string]struct{}{}
		an.idx.walk(func(e *readyEntry) {
			if e.FileHash == "" {
				return
			}
			if _, ok := seen[e.FileHash]; ok {
				return
			}
			seen[e.FileHash] = struct{}{}
			fh, err := xet.ParseFileHash(e.FileHash)
			if err != nil {
				return
			}
			sh, err := an.stor.GetShard(an.ctx, fh)
			if err != nil || sh == nil {
				return
			}
			for _, cb := range sh.CASInfos {
				if n := uint32(len(cb.Chunks)); n > 0 {
					hash := cb.CASHash.String()
					spans[hash] = append(spans[hash], tracker.ChunkSpan{Start: 0, End: n})
				}
			}
		})
	}
	return mergedXorbAdds(spans)
}

func (an *announcer) announce(files []tracker.FileAdd, xorbs []tracker.XorbAdd) {
	if len(files) == 0 && len(xorbs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(an.ctx, 10*time.Second)
	defer cancel()
	_ = an.client.Announce(ctx, &tracker.AnnounceRequest{
		Peer: an.peer,
		TTL:  int64(announceTTL / time.Second),
		Adds: tracker.AnnounceSet{Files: files, Xorbs: xorbs},
	})
}
