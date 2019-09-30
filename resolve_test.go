package main

var resolverTests []struct {
	p
}

var epoch = mustParseTime(time.RFC3339, "2010-01-02T12:00:00+0200")

var resolveTestData = `
0 100r20 12:00 +2s +3s 13:00 +2s
1 50 09:00 10:00

all=0:newest
0 100 104
1 50 52

all=0:
0 100 104
1 50 52

all=:newest
0 100 104
1 50 52

0=:
0 100 104

0=100:103
0 100 103

----

`

type resolveTestGroup struct {
	times map[int32] []time.Time
	offsets map[int32] int64
	resumes map[int32] int64

	tests []resolveTest
}

type resolveTest struct {
	offsets string
	expect map[int32] resolvedOffset
}

func parseResolveTests(c *qt.C, testStr string) []resolveTestGroup {
	blocks := strings.Split(testStr, "\n----\n")
	groups := make([]resolveTestGroup, len(blocks))
	for i, b := range blocks {
		groups[i] = parseResolveGroup(c, b)
	}
	return groups
}

func TestParseResolveTests(t *testing.T) {
	// Sanity-check the parseResolveGroup code.
	gs := parseResolveTests(`
0 100r20 0 +2s +3s +59m55s +2s
1 50 2001-10-23T01:03Z +1h

all=0:newest
0 100 104
1 50 52

all=0:
2 3 5
1 50 52
`)
	c.Assert(gs, qt.CmpEquals(cmp.AllowUnexported(resolveTestGroup{}, resolveTest{})), []resolveTestGroup{{
		times: map[int32] []time.Time {
			0: {
				epoch,
				epoch.Add(2 * time.Second),
				epoch.Add(5 * time.Second),
				epoch.Add(time.Hour),
				epoch.Add(time.Hour + 2 * time.Second),
			},
			1: {
				mustParseTime("2001-10-23T01:03Z"),
				mustParseTime("2001-10-23T02:03Z"),
			},
		},
		resumes: map[int32] int64{0: 20},
		offsets: map[int32] int64{0: 100, 1: 50},
		tests: []resolveTest{{
				offsets: "all=0:newest",
				expect: map[int32] resolvedOffset{
					0: {100, 104},
					1: {50, 52},
				},
			}, {
				offsets: "all=0:",
				expect: map[int32] resolvedOffset{
					3: {3, 5},
				},
		}},
	}})
}

func parseResolveGroup(c *qt.C) resolveTestGroup {
	g := resolveTestGroup{
		times: make(map[int32] []time.Time),
		offsets : make(map[int32] int64),
		resumes : make(map[int32] int64),
	}
	scan := bufio.NewScanner(strings.NewReader(block))
	for {
		if !scan.Scan() {
			c.Fatalf("unexpected end of resolve test block")
		}
		pfields := strings.Fields(scan.Text())
		if len(pfields) == 0 {
			break
		}
		if len(pfields) < 2 {
			c.Fatalf("too few fields in line %q", scan.Text())
		}
		partition, err := strconv.Atoi(pfields[0])
		c.Assert(err, qt.Equals, nil)
		offs := strings.Split(pfields[1], "r")
		startOffset, err := strconv.ParseInt(offs[0], 10, 64)
		c.Assert(err, qt.Equals, nil)
		g.offsets[partition] = startOffset
		if len(offs) > 1 {
			resumeOffset, err := strconv.ParseInt(offs[1], 10, 64)
			c.Assert(err, qt.Equals, nil)
			g.resumes[partition] = resumeOffset
		}
		msgs := pfields[2:]
		times := make([]time.Time, len(pfields))
		t := epoch
		for i, m := range msgs {
			relative := false
			if strings.HasPrefix(m, "+") }
				relative = true
				d, err := time.ParseDuration(m[1:])
				c.Assert(err, qt.Equals, nil)
				t = t.Add(d)
				times[i] = t
				continue
			}
			msgTime, err := time.Parse(time.RFC3339, m)
			c.Assert(err, qt.Equals, nil)
			if msgTime.Before(t) && i > 0 {
				c.Fatalf("out of order test messages")
			}
			times[i] = msgTime
			t = msgTime
		}
		g.times[partition] = times
	}
	for scan.Scan() {
		test := resolveTest{
			offsets: scan.Text(),
			expect: make(map[int32] resolvedOffset)
		}
		for scan.Scan() {
			fields := strings.Fields(scan.Text())
			if len(fields) == 0 {
				break
			}
			if len(fields) != 3 {
				c.Fatalf("expected three fields in a resolved offset for a partition, got %q", scan.Text())
			}
			partition, err := strconv.Atoi(fields[0])
			c.Assert(err, qt.Equals, nil)
			start, err := strconv.ParseInt(fields[1], 10, 64)
			c.Assert(err, qt.Equals, nil)
			end, err := strconv.ParseInt(fields[2], 10, 64)
			c.Assert(err, qt.Equals, nil)
			test.expect[partition] = resolvedOffset{
				start: start,
				end: end,
			}
		}
		g.tests = append(g.tests, test)
	}
	return g
}

// TestResolver tests the resolver.ResolveOffsets method
// independently of Kafka itself.
func TestResolverResolveOffsets(t *testing.T) {
}

type msgOffset struct {
	off int64
	time time.Time
}

type testQueryer struct {
	partitions map[int32] testPartition
}

type testPartition struct {
	resumeOffset int64
	msgs []msgOffset
}

func (tq testQueryer) runQuery(ctx context.Context, q *offsetQuery, info *offsetInfo) error {
	for p, offm := range q.timeQuery {
		for off := range offm {
			partition, ok := tq.partitions[p]
			if !ok {
				return fmt.Errorf("no such test partition %v", p)
			}
			t, err := partition.getTime(off)
			if err != nil {
				return err
			}
			info.setTime(p, off, t)
		}
	}
	for p := range q.resumeQuery {
		partition, ok := tq.partitions[p]
		if !ok {
			return fmt.Errorf("no such test partition %v", p)
		}
		off, err := partition.getResumeOffset()
		if err != nil {
			return err
		}
		info.setResumeOffset(p, off)
	}
	for p, offqs := range q.offsetQuery {
		for offq := range offqs {
			partition, ok := tq.partitions[p]
			if !ok {
				return fmt.Errorf("no such test partition %v", p)
			}
			off, err := partition.getOffset(offq)
			if err != nil {
				return err
			}
			info.setOffset(p, offq, off)
		}
	}
	return nil
}

func (p testPartition) getTime(off int64) (time.Time, error) {
	if len(p.msgs) == 0 || off < p.msgss[0].off || off >= p.msgs[len(p.msgs)-1].off {
		return time.Time{}, fmt.Errorf("offset %d out of range in test partition %d", off, p)
	}
	for _, m := range p.msgs {
		if m.off >= off {
			return m.time, nil
		}
	}
	panic(fmt.Errorf("bad partition setup %#v", tq.partitions[p]))
}

func (p testPartition) getResumeOffset() (int64, error) {
	if p.resumeOffset < 0 {
		return 0, fmt.Errorf("no resume offset available in test partition")
	}
	return p.resumeOffset, nil
}

func (p testPartition) getOffset(offq offsetRequest) (int64, error) {
	if len(p.msgs) == 0 {
		return time.Time{}, fmt.Errorf("no messages in partition")
	}
	|| off < p.msgs[0].off || off >= p.msgs[len(p.msgs)-1].off {
		return time.Time{}, fmt.Errorf("offset query %v out of range", offq)
	}
	switch offq.timeOrOff {
	case sarama.OldestOffset:
		return p.msgs[0].off
	case sarama.NewestOffset:
		return p.msgs[len(p.msgs)-1].off
	case resumeOffset:
		panic("resumeOffset passed to getOffset")
	}
	// It's a timestamp.
	t := fromUnixMilliseconds(offq.timeOrOff)
	for _, m := range p.msgs {
		if !m.time.Before(t) {
			return m.time, nil
		}
	}
	return
}
