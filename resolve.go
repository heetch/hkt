package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
)

type resolver struct {
	topic         string
	info          offsetInfo
	allPartitions map[int32]bool

	client   sarama.Client
	consumer sarama.Consumer
}

type resolvedInterval struct {
	start, end int64
}

func newResolver() *resolver {
	return &resolver{
		info: offsetInfo{
			offsets: make(map[int32]map[offsetRequest]int64),
			resumes: make(map[int32]int64),
			times:   make(map[int32]map[int64]time.Time),
		},
	}
}

func (r *resolver) resolveOffsets(ctx context.Context, offsets map[int32]interval) (map[int32]resolvedInterval, error) {
	allOffsets := make(map[int32]*interval)
	for p, intv := range offsets {
		intv := intv
		allOffsets[p] = &intv
		if p == -1 {
			continue
		}
		if _, ok := r.allPartitions[p]; !ok {
			return nil, fmt.Errorf("partition %v does not exist", p)
		}
	}
	if intv, ok := offsets[-1]; ok {
		// There's an entry that signifies all partitions, so get all the partitions and fill out the map.
		for p := range r.allPartitions {
			if _, ok := allOffsets[p]; !ok {
				intv := intv
				allOffsets[p] = &intv
			}
		}
	}
	resolved := make(map[int32]resolvedInterval)
	// Some offsets can't be resolved in one query (for example,
	// a offset like "latest-1h" will first need to resolve "latest"
	// to the latest offset for the partition, then find the timestamp
	// of the latest message in that partition, then find the offset
	// of that timestamp less one house). So we keep on running
	// queries until there are no more left, moving items out of
	// allOffsets and into resolved when they're fully resolved.
	for len(allOffsets) > 0 {
		q := &offsetQuery{
			timeQuery:   make(map[int32]map[int64]bool),
			offsetQuery: make(map[int32]map[offsetRequest]bool),
			resumeQuery: make(map[int32]bool),
		}

		// We want to minimise the number of queries we make to the
		// server, so instead of querying each position directly, we
		// build up all the query requirements in q, then call runOffsetQuery
		// which will build bulk API calls as needed, and fill in entries
		// inside info, which can then be used to satisfy subsequent information
		// requirements in resolvePosition.
		for p, intv := range allOffsets {
			start, ok1 := resolvePosition(p, intv.start, &r.info, q)
			end, ok2 := resolvePosition(p, intv.end, &r.info, q)
			intv.start, intv.end = start, end
			if ok1 && ok2 {
				// The interval has been fully resolved.
				// Remove it from allOffsets and add it to resolved.
				delete(allOffsets, p)
				resolved[p] = resolvedInterval{
					start: start.anchor.offset,
					end:   end.anchor.offset,
				}
			}
		}
		if err := r.runOffsetQuery(ctx, q); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

// resolvePosition tries to resolve p in the given partition from information provided in info.
// It reports whether the position has been successfully resolved if it succeeds.
// On failure, it will have added at least one thing to be queried to q in
// order to proceed with the resolution.
//
// The returned position is always valid, and may contain updated information.
func resolvePosition(partition int32, p position, info *offsetInfo, q *offsetQuery) (position, bool) {
	if !p.anchor.isTime && p.anchor.offset >= 0 && !p.diff.isDuration && p.diff.offset == 0 {
		return p, true
	}
	if p.anchor.isTime {
		if p.diff.isDuration {
			panic("position has time anchor and duration diff")
		}
		off, ok := info.getOffset(partition, timeOffsetRequest(p.anchor.time), q)
		if !ok {
			return p, false
		}
		p.anchor.isTime = false
		p.anchor.offset = off
		if off >= 0 {
			p.anchor.offset += p.diff.offset
			p.diff = anchorDiff{}
			return p, true
		}
		// The time has resolved to a symbolic offset, which happens
		// when the time query is beyond the end of the topic.
		// Let the offset resolving logic below deal with that.
	}
	if p.anchor.offset < 0 {
		off, ok := info.getOffset(partition, symbolicOffsetRequest(p.anchor.offset), q)
		if !ok {
			return p, false
		}
		p.anchor.offset = off
	}
	if !p.diff.isDuration {
		p.anchor.offset += p.diff.offset
		p.diff = anchorDiff{}
		return p, true
	}
	// It's a non-symbolic offset anchor with a duration diff.
	// Find the time for the offset.
	t, ok := info.getTime(partition, p.anchor.offset, q)
	if !ok {
		return p, false
	}
	// Then get the offset for the anchor time plus the anchor diff duration.
	off, ok := info.getOffset(partition, timeOffsetRequest(t.Add(p.diff.duration)), q)
	if !ok {
		return p, false
	}
	p.anchor.offset = off
	p.diff = anchorDiff{}
	if off >= 0 {
		return p, true
	}
	// The time query has resulted in a symbolic offset, so we need to
	// find the absolute offset for it.
	off1, ok := info.getOffset(partition, symbolicOffsetRequest(off), q)
	if !ok {
		return p, false
	}
	p.anchor.offset = off1
	return p, true
}

// offsetQuery represents a set of information to be asked as bulk requests
// to the Kafka API.
type offsetQuery struct {
	timeQuery   map[int32]map[int64]bool
	offsetQuery map[int32]map[offsetRequest]bool
	resumeQuery map[int32]bool
}

type offsetInfo struct {
	// offsets maps from partition to offset request to offset.
	// This holds results from both time-based queries and symbolic
	// offset queries (as determined by the offset request itself).
	//
	// Note that the resulting offset value may itself by symbolic,
	// as a request for a time beyond the last available time
	// may result in a reference to the last available offset.
	offsets map[int32]map[offsetRequest]int64

	// resumes maps from partition to the resume offset for that partition.
	// This is kept separately from offsets because it's updated by
	// a different goroutine so we can avoid a mutex.
	resumes map[int32]int64

	// offsetTimes maps from partition to offset to the timestamp for that offset in that partition.
	times map[int32]map[int64]time.Time
}

// getOffset returns the offset for a given time or symbolic offset.
func (info *offsetInfo) getOffset(p int32, req offsetRequest, q *offsetQuery) (int64, bool) {
	if req.timeOrOff >= 0 {
		panic("getOffset called with non-symbolic offset")
	}
	if req.timeOrOff == offsetResume {
		// Resume offset queries are resolved with a different kind of API request,
		// so they go into a different field in the query.
		if off, ok := info.resumes[p]; ok {
			return off, true
		}
		q.resumeQuery[p] = true
		return 0, false
	}
	m := q.offsetQuery[p]
	if m == nil {
		m = make(map[offsetRequest]bool)
		q.offsetQuery[p] = m
	}
	m[req] = true
	return 0, false
}

func (info *offsetInfo) getTime(p int32, off int64, q *offsetQuery) (time.Time, bool) {
	// TODO Do we need to find the offset of the last entry in the stream
	// so that we can avoid blocking?
	if off < 0 {
		panic("getTime called with symbolic offset")
	}
	t, ok := info.times[p][off]
	if ok {
		return t, true
	}
	m := q.timeQuery[p]
	if m == nil {
		m = make(map[int64]bool)
		q.timeQuery[p] = m
	}
	m[off] = true
	return time.Time{}, false
}

func (r *resolver) runOffsetQuery(ctx context.Context, q *offsetQuery) error {
	egroup, ctx := errgroup.WithContext(ctx)
	egroup.Go(func() error {
		return r.getTimes(ctx, q.timeQuery)
	})
	egroup.Go(func() error {
		return r.getOffsets(ctx, q.offsetQuery)
	})
	egroup.Go(func() error {
		return r.getResumes(ctx, q.resumeQuery)
	})
	if err := egroup.Wait(); err != nil {
		return err
	}
	return nil
}

func (r *resolver) getTimes(ctx context.Context, q map[int32]map[int64]bool) error {
	type result struct {
		partition int32
		offset    int64
		time      time.Time
	}
	resultc := make(chan result)
	nrequests := 0
	egroup, ctx := errgroup.WithContext(ctx)
	for p, offsets := range q {
		p := p
		for offset := range offsets {
			offset := offset
			if offset < 0 {
				panic("symbolic offset passed to getTime")
			}
			nrequests++
			egroup.Go(func() error {
				// We'd probably be better off calling Fetch directly, because
				// ConsumePartition will immediately fire off another unnecessary request
				// before we close it.
				pconsumer, err := r.consumer.ConsumePartition(r.topic, p, offset)
				if err != nil {
					return err
				}
				defer pconsumer.Close()
				select {
				case m, ok := <-pconsumer.Messages():
					if !ok {
						return fmt.Errorf("closed messages channel")
					}
					resultc <- result{
						partition: p,
						offset:    offset,
						time:      m.Timestamp,
					}
				case err := <-pconsumer.Errors():
					// TODO what do we do about errors?
					_ = err
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			})
		}
	}
	egroup.Go(func() error {
		for i := 0; i < nrequests; i++ {
			select {
			case result := <-resultc:
				m := r.info.times[result.partition]
				if m == nil {
					m = make(map[int64]time.Time)
					r.info.times[result.partition] = m
				}
				m[result.offset] = result.time
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	return egroup.Wait()
}

func (r *resolver) getResumes(ctx context.Context, q map[int32]bool) error {
	return fmt.Errorf("not yet implemented")
	//	for p := range q {
	//		// get partition manager for partition
	//
	//		info.resumes[p] = pom.NextOffset()
	//	}
	//	return nil
}

// getOffsets gets offsets for all the requests in q and stores the results in r.info.offsets.
// Note that this does not resolve resume offset requests, which are handled by getResumes.
func (r *resolver) getOffsets(ctx context.Context, q map[int32]map[offsetRequest]bool) error {
	type result struct {
		askFor map[int32]offsetRequest
		resp   *sarama.OffsetResponse
		err    error
	}
	// This is unbuffered so if we fail, we'll leave some dangling goroutines that will block trying to
	// write to resultc, but as the whole command is going to exit in that case, we don't care.
	resultc := make(chan result)

	nqueries := 0
	// We can only ask about one partition per request, so keep issuing requests
	// until we have no more left to ask about.
	for len(q) > 0 {
		// We're going to issue a request for each unique broker that manages
		// any of the partitions in the query.
		reqs := make(map[*sarama.Broker]*sarama.OffsetRequest)

		// askFor records the partitions that we're asking about,
		// so we can make sure the broker has responded with
		// the expected information, otherwise there's a risk
		// that a broker with broken behaviour could cause
		// getOffsetsForTimes to fail to fill out the required info
		// in info, which could cause an infinite loop in the calling
		// logic which expects all queries to get an answer.
		askFor := make(map[*sarama.Broker]map[int32]offsetRequest)
		for p, offqs := range q {
			if len(offqs) == 0 {
				// Defensive: this shouldn't happen.
				delete(q, p)
				continue
			}
			var offq offsetRequest
			for offq = range offqs {
				delete(offqs, offq)
				break
			}
			if len(offqs) == 0 {
				delete(q, p)
			}
			if offq.timeOrOff == offsetResume {
				panic("getOffsets called with offsetResume query")
			}
			b, err := r.client.Leader(r.topic, p)
			if err != nil {
				return err
			}
			req := reqs[b]
			if req == nil {
				req = &sarama.OffsetRequest{
					Version: 1,
				}
				reqs[b] = req
				askFor[b] = make(map[int32]offsetRequest)
			}
			req.AddBlock(r.topic, p, offq.timeOrOff, 1)
			askFor[b][p] = offq
		}
		for b, req := range reqs {
			b, req := b, req
			nqueries++
			go func() {
				// We should pass ctx here but sarama doesn't support context.
				resp, err := b.GetAvailableOffsets(req)
				resultc <- result{
					askFor: askFor[b],
					resp:   resp,
					err:    err,
				}
			}()
		}
	}
	for i := 0; i < nqueries; i++ {
		select {
		case result := <-resultc:
			ps, ok := result.resp.Blocks[r.topic]
			if !ok {
				return fmt.Errorf("topic %q not found in offsets response", r.topic)
			}
			for p, offq := range result.askFor {
				block, ok := ps[p]
				if !ok {
					return fmt.Errorf("no offset found for partition %d", p)
				}
				if block.Err != 0 {
					return fmt.Errorf("cannot get offset for partition %d: %v", p, block.Err)
				}
				if block.Offset < 0 {
					// This happens when the time is beyond the last offset.
					block.Offset = sarama.OffsetNewest // TODO or newestOffset?
				}
				r.info.offsets[p][offq] = block.Offset
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type offsetRequest struct {
	// timeOrOff holds the number of milliseconds since Jan 1st 1970 of the
	// offset to request, or one of offsetResume, sarama.OldestOffset, sarama.NewestOffset
	// if it's not a time-based request.
	//
	// Note that this is in the same form expected by the ListOffset Kakfa API.
	timeOrOff int64
}

func timeOffsetRequest(t time.Time) offsetRequest {
	return offsetRequest{
		timeOrOff: unixMilliseconds(t),
	}
}

func symbolicOffsetRequest(off int64) offsetRequest {
	if off >= 0 {
		panic("symbolicOffsetRequest called with non-symbolic offset")
	}
	return offsetRequest{
		timeOrOff: off,
	}
}

func unixMilliseconds(t time.Time) int64 {
	ns := time.Duration(t.UnixNano())
	return int64(ns / time.Millisecond)
}

func fromUnixMilliseconds(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts/1000, (ts%1000)*1e6)
}
