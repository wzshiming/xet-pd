package agent

import (
	"sync"
	"time"

	"github.com/wzshiming/xet-pd/tracker"
)

const (
	// maxQuarantineFiles caps how many files may hold quarantined segments
	// at once; files in flight are few, the cap is defensive.
	maxQuarantineFiles = 256
	// maxQuarantineItems caps the segments tracked per file.
	maxQuarantineItems = 4096
	// quarantineTTL is how long a registered segment may await its verdict
	// before the sweep purges it as belonging to a flight that never
	// settled (crashed recorder, shutdown race, dropped registry entry).
	quarantineTTL = time.Hour
	// sweepInterval is the cadence of the agent's quarantine sweeper; a
	// few-minute cadence keeps the expired backlog small against the
	// hour-long TTL.
	sweepInterval = 5 * time.Minute
)

// quarItem is one quarantined segment awaiting its file's checksum verdict,
// remembering which peer supplied it so a failed verdict can cool the peer.
// partial marks items whose source fetch entry was wider than the covered
// union: the client's decoded-chunk cache then holds chunks the verdict
// never validated, so even a passing verdict must purge that cache.
type quarItem struct {
	xorbHash string
	span     tracker.ChunkSpan
	peer     string
	partial  bool
	// added is when the item entered the registry, driving the sweep.
	added time.Time
}

// quarantine tracks peer-sourced segments per reconstruction file hash until
// the ingest consuming them settles: success releases them for announcing,
// failure purges them. In-memory only — a registry record is the proof the
// segment's bytes were actually served into the flight the verdict hashed,
// so only registered segments may ever be released. Segments whose record is
// lost (restart, cap refusal, failed settle) stay invisible behind their
// persisted quarantined meta until the sweep purges them from it.
type quarantine struct {
	mu    sync.Mutex
	files map[string][]quarItem
	order []string
}

func newQuarantine() *quarantine {
	return &quarantine{files: map[string][]quarItem{}}
}

// add registers quarantined segments under their file hash atomically:
// either every item is registered or none is, so a partially registered
// fetch can never be served. Items dedup by segment key — a re-add under
// the same file is a success. It reports whether all items are registered;
// a full registry or full file refuses new items instead of silently
// evicting entries a live flight still needs — callers fall back to the
// origin, and the sweep frees slots over time.
func (q *quarantine) add(fileHash string, items ...quarItem) bool {
	if len(items) == 0 {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	existing, ok := q.files[fileHash]
	if !ok && len(q.files) >= maxQuarantineFiles {
		return false
	}
	registered := func(list []quarItem, item quarItem) bool {
		for _, it := range list {
			if it.xorbHash == item.xorbHash && it.span == item.span {
				return true
			}
		}
		return false
	}
	now := time.Now()
	fresh := make([]quarItem, 0, len(items))
	for _, item := range items {
		if registered(existing, item) || registered(fresh, item) {
			continue
		}
		item.added = now
		fresh = append(fresh, item)
	}
	if len(existing)+len(fresh) > maxQuarantineItems {
		return false
	}
	if len(fresh) == 0 {
		return true
	}
	q.files[fileHash] = append(existing, fresh...)
	if !ok {
		q.order = append(q.order, fileHash)
	}
	return true
}

// expire removes and returns every item registered before cutoff, so
// flights that never settle cannot pin quarantined segments forever.
func (q *quarantine) expire(cutoff time.Time) []quarItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	var expired []quarItem
	for fileHash, items := range q.files {
		kept := items[:0]
		for _, it := range items {
			if it.added.Before(cutoff) {
				expired = append(expired, it)
			} else {
				kept = append(kept, it)
			}
		}
		if len(kept) == 0 {
			delete(q.files, fileHash)
		} else {
			q.files[fileHash] = kept
		}
	}
	if len(expired) > 0 {
		order := q.order[:0]
		for _, fh := range q.order {
			if _, ok := q.files[fh]; ok {
				order = append(order, fh)
			}
		}
		q.order = order
	}
	return expired
}

// take removes and returns the items registered under fileHash.
func (q *quarantine) take(fileHash string) []quarItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	items, ok := q.files[fileHash]
	if !ok {
		return nil
	}
	delete(q.files, fileHash)
	for i, fh := range q.order {
		if fh == fileHash {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}
	return items
}
