package main

import (
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
)

func TestParseOffsets(t *testing.T) {
	data := []struct {
		testName    string
		input       string
		expected    map[int32]interval
		expectedErr string
	}{{
		testName: "empty",
		input:    "",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-comma",
		input:    ",",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all",
		input:    "all",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest",
		input:    "oldest",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "resume",
		input:    "resume",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(offsetResume),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-with-space",
		input: "	all ",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-with-zero-initial-offset",
		input:    "all=+0:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "several-partitions",
		input:    "1,2,4",
		expected: map[int32]interval{
			1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
			2: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
			4: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,empty-offsets",
		input:    "0=",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,one-offset",
		input:    "0=1",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,empty-after-colon",
		input:    "0=1:",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "multiple-partitions",
		input:    "0=4:,2=1:10,6",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(4),
				end:   positionAtOffset(maxOffset),
			},
			2: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(10),
			},
			6: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "newest-relative",
		input:    "0=-1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "newest-relative,empty-after-colon",
		input:    "0=-1:",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "resume-relative",
		input:    "0=resume-10",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(offsetResume),
					diff:   anchorDiff{offset: -10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative",
		input:    "0=+1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative,empty-after-colon",
		input:    "0=+1:",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative-to-newest-relative",
		input:    "0=+1:-1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
			},
		},
	}, {
		testName: "specific-partition-with-all-partitions",
		input:    "0=+1:-1,all=1:10",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
			},
			-1: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(10),
			},
		},
	}, {
		testName: "oldest-to-newest",
		input:    "0=oldest:newest",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(sarama.OffsetNewest),
			},
		},
	}, {
		testName: "oldest-to-newest-with-offsets",
		input:    "0=oldest+10:newest-10",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
			},
		},
	}, {
		testName: "newest",
		input:    "newest",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetNewest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-partition",
		input:    "10",
		expected: map[int32]interval{
			10: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-range,all-partitions",
		input:    "10:20",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(10),
				end:   positionAtOffset(20),
			},
		},
	}, {
		testName: "single-range,all-partitions,open-end",
		input:    "10:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(10),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-newest",
		input:    "all=newest:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetNewest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-newest-with-offset",
		input:    "newest-10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-oldest-with-offset",
		input:    "oldest+10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-neg-offset-empty-colon",
		input:    "-10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-pos-offset-empty-colon",
		input:    "+10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "start-offset-combines-with-diff-offset",
		input:    "1000+3",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(1003),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName:    "invalid-partition",
		input:       "bogus",
		expectedErr: `invalid anchor position "bogus"`,
	}, {
		testName:    "several-colons",
		input:       ":::",
		expectedErr: `invalid position ":::"`,
	}, {
		testName:    "bad-relative-offset-start",
		input:       "foo+20",
		expectedErr: `invalid anchor position "foo"`,
	}, {
		testName:    "bad-relative-offset-diff",
		input:       "oldest+bad",
		expectedErr: `invalid relative position "\+bad"`,
	}, {
		testName:    "bad-relative-offset-diff-at-start",
		input:       "+bad",
		expectedErr: `invalid relative position "\+bad"`,
	}, {
		testName:    "relative-offset-too-big",
		input:       "+9223372036854775808",
		expectedErr: `offset "\+9223372036854775808" is too large`,
	}, {
		testName:    "starting-offset-too-big",
		input:       "9223372036854775808:newest",
		expectedErr: `anchor offset "9223372036854775808" is too large`,
	}, {
		testName:    "ending-offset-too-big",
		input:       "oldest:9223372036854775808",
		expectedErr: `anchor offset "9223372036854775808" is too large`,
	}, {
		testName:    "partition-too-big",
		input:       "2147483648=oldest",
		expectedErr: `partition number "2147483648" is too large`,
	}, {
		testName: "time-anchor-rfc3339",
		input:    "[2019-08-31T13:06:08.234Z]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T13:06:08.234Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-rfc3339-not-utc",
		input:    "[2019-08-31T13:06:08.234-04:00]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T17:06:08.234Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-date",
		input:    "[2019-08-31]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtTime(T("2019-09-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-imprecise-explicit-colon",
		input:    "[2019-08-31]:",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-date-explicit-end",
		input:    "[2019-08-31]:[2019-09-04]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtTime(T("2019-09-05T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-month",
		input:    "[2019-08]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-01T00:00:00Z")),
				end:   positionAtTime(T("2019-09-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-year",
		input:    "[2019]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-01-01T00:00:00Z")),
				end:   positionAtTime(T("2020-01-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-minute",
		input:    "[13:45]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T13:45:00Z")),
				end:   positionAtTime(T("2011-02-03T13:46:00Z")),
			},
		},
	}, {
		testName: "time-anchor-second",
		input:    "[13:45:12.345]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T13:45:12.345Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-hour",
		input:    "[4pm]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T16:00:00Z")),
				end:   positionAtTime(T("2011-02-03T17:00:00Z")),
			},
		},
	}, {
		testName: "time-range",
		input:    "[2019-08-31T13:06:08.234Z]:[2023-02-05T12:01:02.6789Z]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T13:06:08.234Z")),
				end:   positionAtTime(T("2023-02-05T12:01:02.6789Z")),
			},
		},
	}, {
		testName: "time-anchor-with-diff-offset",
		input:    "[4pm]-123",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtTime(T("2011-02-03T16:00:00Z")),
					diff:   anchorDiff{offset: -123},
				},
				end: position{
					anchor: anchorAtTime(T("2011-02-03T17:00:00Z")),
					diff:   anchorDiff{offset: -123},
				},
			},
		},
	}, {
		testName: "offset-anchor-with-negative-time-rel",
		input:    "1234-1h3s",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtOffset(1234),
					diff: anchorDiff{
						isDuration: true,
						duration:   -(time.Hour + 3*time.Second),
					},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "offset-anchor-with-positive-time-rel",
		input:    "1234+555ms",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtOffset(1234),
					diff: anchorDiff{
						isDuration: true,
						duration:   555 * time.Millisecond,
					},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-combined-with-time-rel",
		input:    "[3pm]+5s",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T15:00:05Z")),
				end:   positionAtTime(T("2011-02-03T16:00:05Z")),
			},
		},
	},
	// TODO error cases
	// TODO local time resolution
	}
	c := qt.New(t)
	// Choose a reference date that's not UTC, so we can ensure
	// that the timezone-dependent logic works correctly.
	now := T("2011-02-03T16:05:06.500Z").In(time.FixedZone("UTC-8", -8*60*60))
	for _, d := range data {
		c.Run(d.testName, func(c *qt.C) {
			actual, err := parseOffsets(d.input, now)
			if d.expectedErr != "" {
				c.Assert(err, qt.ErrorMatches, d.expectedErr)
				return
			}
			c.Assert(err, qt.Equals, nil)
			c.Assert(actual, deepEquals, d.expected)
		})
	}
}

func TestFindPartitionsToConsume(t *testing.T) {
	data := []struct {
		testName string
		topic    string
		offsets  map[int32]interval
		consumer tConsumer
		expected []int32
	}{{
		testName: "one-partition",
		topic:    "a",
		offsets: map[int32]interval{
			10: {
				start: positionAtOffset(2),
				end:   positionAtOffset(4),
			},
		},
		consumer: tConsumer{
			topics:              []string{"a"},
			topicsErr:           nil,
			partitions:          map[string][]int32{"a": {0, 10}},
			partitionsErr:       map[string]error{"a": nil},
			consumePartition:    map[tConsumePartition]tPartitionConsumer{},
			consumePartitionErr: map[tConsumePartition]error{},
			closeErr:            nil,
		},
		expected: []int32{10},
	}, {
		testName: "all-partitions",
		topic:    "a",
		offsets: map[int32]interval{
			-1: {
				start: positionAtOffset(3),
				end:   positionAtOffset(41),
			},
		},
		consumer: tConsumer{
			topics:              []string{"a"},
			topicsErr:           nil,
			partitions:          map[string][]int32{"a": {0, 10}},
			partitionsErr:       map[string]error{"a": nil},
			consumePartition:    map[tConsumePartition]tPartitionConsumer{},
			consumePartitionErr: map[tConsumePartition]error{},
			closeErr:            nil,
		},
		expected: []int32{0, 10},
	}}
	c := qt.New(t)
	for _, d := range data {
		c.Run(d.testName, func(c *qt.C) {
			target := &consumeCmd{
				consumer: d.consumer,
				topic:    d.topic,
				offsets:  d.offsets,
			}
			actual, err := target.findPartitions()
			c.Assert(err, qt.Equals, nil)
			c.Assert(actual, qt.DeepEquals, d.expected)
		})
	}
}

func TestConsume(t *testing.T) {
	c := qt.New(t)
	closer := make(chan struct{})
	messageChan := make(<-chan *sarama.ConsumerMessage)
	calls := make(chan tConsumePartition)
	consumer := tConsumer{
		consumePartition: map[tConsumePartition]tPartitionConsumer{
			tConsumePartition{"hans", 1, 1}: tPartitionConsumer{messages: messageChan},
			tConsumePartition{"hans", 2, 1}: tPartitionConsumer{messages: messageChan},
		},
		calls: calls,
	}
	target := consumeCmd{consumer: consumer}
	target.topic = "hans"
	target.brokers = []string{"localhost:9092"}

	go target.consume(map[int32]resolvedInterval{
		1: {1, 5},
		2: {1, 5},
	})
	defer close(closer)

	var actual []tConsumePartition
	expected := []tConsumePartition{
		tConsumePartition{"hans", 1, 1},
		tConsumePartition{"hans", 2, 1},
	}
	timeout := time.After(time.Second)
	for {
		select {
		case call := <-calls:
			actual = append(actual, call)
			if len(actual) < len(expected) {
				break
			}
			sort.Sort(ByPartitionOffset(actual))
			c.Check(actual, qt.DeepEquals, expected)
			return
		case <-timeout:
			c.Fatalf("Did not receive calls to consume partitions before timeout.")
		}
	}
}

type tConsumePartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type ByPartitionOffset []tConsumePartition

func (a ByPartitionOffset) Len() int {
	return len(a)
}
func (a ByPartitionOffset) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByPartitionOffset) Less(i, j int) bool {
	if a[i].Partition != a[j].Partition {
		return a[i].Partition < a[j].Partition
	}
	return a[i].Offset < a[j].Offset
}

type tPartitionConsumer struct {
	closeErr            error
	highWaterMarkOffset int64
	messages            <-chan *sarama.ConsumerMessage
	errors              <-chan *sarama.ConsumerError
}

func (pc tPartitionConsumer) AsyncClose() {}
func (pc tPartitionConsumer) Close() error {
	return pc.closeErr
}
func (pc tPartitionConsumer) HighWaterMarkOffset() int64 {
	return pc.highWaterMarkOffset
}
func (pc tPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}
func (pc tPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

type tConsumer struct {
	topics              []string
	topicsErr           error
	partitions          map[string][]int32
	partitionsErr       map[string]error
	consumePartition    map[tConsumePartition]tPartitionConsumer
	consumePartitionErr map[tConsumePartition]error
	closeErr            error
	calls               chan tConsumePartition
}

func (c tConsumer) Topics() ([]string, error) {
	return c.topics, c.topicsErr
}

func (c tConsumer) Partitions(topic string) ([]int32, error) {
	return c.partitions[topic], c.partitionsErr[topic]
}

func (c tConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cp := tConsumePartition{topic, partition, offset}
	c.calls <- cp
	return c.consumePartition[cp], c.consumePartitionErr[cp]
}

func (c tConsumer) Close() error {
	return c.closeErr
}

func (c tConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func TestConsumeParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_TOPIC", "test-topic")
	c.Setenv("KT_BROKERS", "hans:2000")

	var target consumeCmd
	target.parseArgs(nil)
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
}

// brokers default to localhost:9092
func TestConsumeParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_TOPIC", "")
	c.Setenv("KT_BROKERS", "")

	var target consumeCmd
	target.parseArgs([]string{"-topic", "test-topic"})
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"localhost:9092"})
}

func TestConsumeParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	// command line arg wins
	c.Setenv("KT_TOPIC", "BLUBB")
	c.Setenv("KT_BROKERS", "BLABB")

	var target consumeCmd
	target.parseArgs([]string{"-topic", "test-topic", "-brokers", "hans:2000"})
	c.Assert(target.topic, qt.Equals, "test-topic")
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
}

func T(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// deepEquals allows comparison of the unexported fields inside the
// struct types that we test internally.
var deepEquals = qt.CmpEquals(cmp.AllowUnexported(
	interval{},
	position{},
	anchor{},
	anchorDiff{},
	message{},
))

func positionAtOffset(off int64) position {
	return position{
		anchor: anchorAtOffset(off),
	}
}

func positionAtTime(t time.Time) position {
	return position{
		anchor: anchorAtTime(t),
	}
}
