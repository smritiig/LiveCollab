package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// This fixture speaks RESP over real TCP connections. Commands share an atomic
// log/state, and XREAD waits on notifications instead of polling with sleeps.
type replayRedis struct {
	mu             sync.Mutex
	events         []DistributedEvent
	changed        chan struct{}
	stop           chan struct{}
	stopOnce       sync.Once
	otherRequested chan int64
	readGate       <-chan struct{}
	requested      chan int64
	offered        chan int64
	pages          int
	replayPaused   chan struct{}
	replayGate     <-chan struct{}
	pageSize       int
}

func newReplayRedis(t *testing.T, count int, gate <-chan struct{}) (*replayRedis, *RedisStore) {
	t.Helper()
	f := &replayRedis{changed: make(chan struct{}), stop: make(chan struct{}), readGate: gate,
		requested: make(chan int64, 128), offered: make(chan int64, 128), otherRequested: make(chan int64, 128)}
	for i := 1; i <= count; i++ {
		f.append(int64(i), fmt.Sprintf("content-%d", i), fmt.Sprintf("op-%d", i), "Alice")
	}
	t.Cleanup(func() { f.stopOnce.Do(func() { close(f.stop) }) })
	return f, f.store(t, true)
}

func (f *replayRedis) store(t *testing.T, observe bool) *RedisStore {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				raw, err := readRESP(bufio.NewReader(conn))
				if err != nil {
					return
				}
				values, ok := raw.([]any)
				if !ok {
					return
				}
				args := make([]string, len(values))
				for i, v := range values {
					args[i] = redisString(v)
				}
				writeTestRESP(conn, f.command(args, observe))
			}()
		}
	}()
	t.Cleanup(func() { f.stopOnce.Do(func() { close(f.stop) }); _ = listener.Close(); wg.Wait() })
	return NewRedisStore(listener.Addr().String())
}

// Caller holds mu after the fixture has been published.
func (f *replayRedis) append(sequence int64, content, operation, client string) DistributedEvent {
	event := DistributedEvent{StreamID: fmt.Sprintf("%d-0", sequence), Sequence: sequence, Version: sequence,
		Content: content, OperationID: operation, ClientID: client, Username: client}
	f.events = append(f.events, event)
	close(f.changed)
	f.changed = make(chan struct{})
	return event
}

func streamEntry(event DistributedEvent) any {
	return []any{event.StreamID, []any{"sequence", strconv.FormatInt(event.Sequence, 10), "version", strconv.FormatInt(event.Version, 10),
		"content", event.Content, "operationId", event.OperationID, "clientId", event.ClientID, "username", event.Username}}
}

func streamNumber(id string) int64 {
	n, _ := strconv.ParseInt(strings.Split(strings.TrimPrefix(id, "("), "-")[0], 10, 64)
	return n
}

func (f *replayRedis) signal(ch chan int64, n int64) {
	select {
	case ch <- n:
	case <-f.stop:
	}
}

func (f *replayRedis) command(a []string, observe bool) any {
	if a[0] == "XREAD" {
		cursor := streamNumber(a[len(a)-1])
		if observe {
			f.signal(f.requested, cursor)
		} else {
			f.signal(f.otherRequested, cursor)
		}
		if f.readGate != nil {
			select {
			case <-f.readGate:
			case <-f.stop:
				return nil
			}
		}
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		for {
			f.mu.Lock()
			entries := []any{}
			last := int64(0)
			for _, e := range f.events {
				if e.Sequence > cursor {
					entries = append(entries, streamEntry(e))
					last = e.Sequence
				}
			}
			changed := f.changed
			f.mu.Unlock()
			if len(entries) > 0 {
				if observe {
					f.signal(f.offered, last)
				}
				return []any{[]any{a[len(a)-2], entries}}
			}
			select {
			case <-changed:
			case <-timer.C:
				return nil
			case <-f.stop:
				return nil
			}
		}
	}
	if a[0] == "XRANGE" {
		f.mu.Lock()
		paused, gate := f.replayPaused, f.replayGate
		f.mu.Unlock()
		if gate != nil && a[2] == "(2-0" {
			close(paused)
			select {
			case <-gate:
			case <-f.stop:
				return nil
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	latest := DistributedEvent{}
	if len(f.events) > 0 {
		latest = f.events[len(f.events)-1]
	}
	switch a[0] {
	case "GET":
		switch {
		case strings.HasSuffix(a[1], ":exists"):
			return "1"
		case strings.HasSuffix(a[1], ":content"):
			return latest.Content
		case strings.HasSuffix(a[1], ":version"):
			return strconv.FormatInt(latest.Version, 10)
		case strings.HasSuffix(a[1], ":sequence"):
			return strconv.FormatInt(latest.Sequence, 10)
		}
	case "XREVRANGE":
		if latest.Sequence == 0 {
			return []any{}
		}
		return []any{streamEntry(latest)}
	case "XRANGE":
		f.pages++
		start, end := streamNumber(a[2]), streamNumber(a[3])
		limit, _ := strconv.Atoi(a[5])
		if f.pageSize > 0 && limit > f.pageSize {
			limit = f.pageSize
		}
		entries := []any{}
		for _, e := range f.events {
			if e.Sequence < start || (strings.HasPrefix(a[2], "(") && e.Sequence == start) || e.Sequence > end {
				continue
			}
			entries = append(entries, streamEntry(e))
			if len(entries) == limit {
				break
			}
		}
		return entries
	case "EVAL":
		argv := a[7:]
		base, _ := strconv.ParseInt(argv[0], 10, 64)
		stale := 0
		if base != latest.Version {
			stale = 1
		}
		if stale == 1 && argv[5] == "reject" {
			return []any{0, latest.Version, latest.Version, latest.Content, latest.Sequence, "", stale, "stale_base_version"}
		}
		e := f.append(latest.Sequence+1, argv[1], argv[2], argv[3])
		return []any{1, latest.Version, e.Version, e.Content, e.Sequence, e.StreamID, stale, ""}
	}
	return fmt.Errorf("unsupported test Redis command %v", a)
}

func writeTestRESP(w io.Writer, value any) {
	switch v := value.(type) {
	case nil:
		fmt.Fprint(w, "$-1\r\n")
	case error:
		fmt.Fprintf(w, "-%s\r\n", v)
	case []any:
		fmt.Fprintf(w, "*%d\r\n", len(v))
		for _, item := range v {
			writeTestRESP(w, item)
		}
	case int:
		fmt.Fprintf(w, ":%d\r\n", v)
	case int64:
		fmt.Fprintf(w, ":%d\r\n", v)
	case string:
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v), v)
	default:
		panic(fmt.Sprintf("unsupported RESP value %T", v))
	}
}

type barrierListener struct {
	net.Listener
	before func(Message)
}

func (l *barrierListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &barrierConn{Conn: c, before: l.before}, nil
}

type barrierConn struct {
	net.Conn
	before func(Message)
}

func (c *barrierConn) Write(p []byte) (int, error) {
	// Small server JSON frames are unmasked and written in one transport write.
	// The HTTP upgrade has no JSON object, so it is ignored here.
	var message Message
	if i := bytes.IndexByte(p, '{'); i >= 0 {
		_ = json.Unmarshal(p[i:], &message)
	}
	if c.before != nil {
		c.before(message)
	}
	n, err := c.Conn.Write(p)
	return n, err
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for barrier")
	}
}
func awaitSequence(t *testing.T, ch <-chan int64, expected int64) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case n := <-ch:
			if n == expected {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for stream cursor %d", expected)
		}
	}
}
func readFrame(t *testing.T, conn *websocket.Conn) Message {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var m Message
	if err := conn.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}
	return m
}
func dialRoom(t *testing.T, server *httptest.Server, username string, cursor int) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + fmt.Sprintf("/ws?roomId=room&username=%s&lastSequence=%d", username, cursor)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestReconnectReplayToLiveSequenceOrder(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap=%t", overlap), func(t *testing.T) {
			gate := make(chan struct{})
			var gateOnce sync.Once
			releaseWatcher := func() { gateOnce.Do(func() { close(gate) }) }
			defer releaseWatcher()
			f, store := newReplayRedis(t, 5, gate)
			config := Config{InstanceID: "backend-a", StaleWritePolicy: StaleWriteReject}
			telemetry := NewTelemetry(config)
			manager := NewDistributedRoomManager(store, config.InstanceID, nil, telemetry)
			defer manager.Close()
			initial := int64(5)
			if overlap {
				initial = 0
			}
			room := &Room{ID: "room", Store: store, Sequence: initial, Version: initial, Content: fmt.Sprintf("content-%d", initial)}
			manager.Rooms[room.ID] = room
			replayPaused, releaseReplay := make(chan struct{}), make(chan struct{})
			handoffPaused, releaseHandoff := make(chan struct{}), make(chan struct{})
			var replayOnce, handoffOnce sync.Once
			defer replayOnce.Do(func() { close(releaseReplay) })
			defer handoffOnce.Do(func() { close(releaseHandoff) })
			server := httptest.NewUnstartedServer(NewApplication(manager, config, nil, telemetry).Handler())
			f.mu.Lock()
			f.pageSize, f.replayPaused, f.replayGate = 2, replayPaused, releaseReplay
			f.mu.Unlock()
			server.Listener = &barrierListener{Listener: server.Listener,
				before: func(m Message) {
					if m.Type == "resume_complete" {
						close(handoffPaused)
						<-releaseHandoff
					}
				}}

			server.Start()
			defer server.Close()
			bob := dialRoom(t, server, "Bob", 1)
			awaitSignal(t, replayPaused)
			clients := room.clientsSnapshot()
			if len(clients) != 1 {
				t.Fatalf("clients=%d", len(clients))
			}
			client := clients[0]

			// Backend B accepts operations through the real WebSocket handler. Its
			// watcher has a separate RESP connection and shares the same committed log.
			configB := config
			configB.InstanceID = "backend-b"
			managerB := NewDistributedRoomManager(f.store(t, false), configB.InstanceID, nil, NewTelemetry(configB))
			defer managerB.Close()
			serverB := httptest.NewServer(NewApplication(managerB, configB, nil, managerB.telemetry).Handler())
			defer serverB.Close()
			alice := dialRoom(t, serverB, "Alice", 0)
			for m := readFrame(t, alice); m.Type != "room_state"; m = readFrame(t, alice) {
			}
			aliceEvents := []int64{}
			commit := func(sequence int64) {
				t.Helper()
				err := alice.WriteJSON(Message{Type: "content_update", OperationID: fmt.Sprintf("op-%d", sequence), BaseVersion: int64Pointer(sequence - 1), Content: fmt.Sprintf("content-%d", sequence)})
				if err != nil {
					t.Fatal(err)
				}
				for {
					m := readFrame(t, alice)
					if m.Type == "content_update" {
						aliceEvents = append(aliceEvents, m.Sequence)
					}
					if m.Type == "operation_result" {
						if m.Accepted == nil || !*m.Accepted || m.Sequence != sequence {
							t.Fatalf("ack: %+v", m)
						}
						return
					}
				}
			}
			commit(6)
			// A snapshot refresh must not move the watcher's fan-out cursor past 6.
			if snapshot := room.GetSnapshot(); snapshot.Sequence != 6 {
				t.Fatalf("snapshot: %+v", snapshot)
			}
			releaseWatcher()
			// A requests its next batch only after completing its broadcasts.
			awaitSequence(t, f.requested, 6)
			client.deliveryMu.Lock()
			bufferedSix := client.replaying && client.pending[6].Sequence == 6
			bufferedOverlap := client.pending[3].Sequence == 3 && client.pending[5].Sequence == 5
			client.deliveryMu.Unlock()
			if !bufferedSix || (overlap && !bufferedOverlap) {
				t.Fatalf("buffer missing: six=%t overlap=%t", bufferedSix, bufferedOverlap)
			}
			replayOnce.Do(func() { close(releaseReplay) })
			awaitSignal(t, handoffPaused)
			commit(7)
			awaitSequence(t, f.offered, 7)
			// The completion frame is blocked while finishReplay owns deliveryMu.
			// Event 7 has entered the live read path during that critical section.
			handoffOnce.Do(func() { close(releaseHandoff) })
			frames := []Message{}
			for {
				m := readFrame(t, bob)
				frames = append(frames, m)
				if m.Type == "content_update" && m.Sequence == 7 {
					break
				}
			}
			client.deliveryMu.Lock()
			live := !client.replaying
			client.deliveryMu.Unlock()
			if !live {
				t.Fatal("client not live after handoff")
			}
			commit(8)
			awaitSequence(t, f.requested, 8)
			awaitSequence(t, f.otherRequested, 8)
			// These control markers follow completed fan-out; inspect all raw frames
			// through the markers, including any duplicate final content frame.
			if err := bob.WriteJSON(Message{Type: "typing"}); err != nil {
				t.Fatal(err)
			}
			roomB, _ := managerB.GetRoom("room")
			roomB.BroadcastJSON(Message{Type: "typing", Username: "end"})
			for {
				m := readFrame(t, bob)
				if m.Type == "typing" && m.Username == "Bob" {
					break
				}
				frames = append(frames, m)
			}
			for {
				m := readFrame(t, alice)
				if m.Type == "typing" && m.Username == "end" {
					break
				}
				if m.Type == "content_update" {
					aliceEvents = append(aliceEvents, m.Sequence)
				}
			}
			sequences := []int64{}
			completed := false
			last := int64(1)
			for _, m := range frames {
				switch m.Type {
				case "resume_started":
					if m.LatestSequence != 5 || m.ServerVersion != 5 {
						t.Fatalf("start: %+v", m)
					}
				case "content_update":
					sequences = append(sequences, m.Sequence)
					last = m.Sequence
				case "resume_complete":
					if completed || m.LatestSequence != 5 || m.ServerVersion != 5 || m.Replayed != 4 || last != 5 {
						t.Fatalf("completion at %d: %+v", last, m)
					}
					completed = true
				}
			}
			expected := []int64{2, 3, 4, 5, 6, 7, 8}
			if !completed || !reflect.DeepEqual(sequences, expected) {
				t.Fatalf("raw sequences=%v, complete=%t", sequences, completed)
			}
			if !reflect.DeepEqual(aliceEvents, []int64{6, 7, 8}) {
				t.Fatalf("same-node fan-out=%v", aliceEvents)
			}
			snapshot := room.GetSnapshot()
			if snapshot.Sequence != 8 || snapshot.Version != 8 || snapshot.Content != "content-8" {
				t.Fatalf("final snapshot: %+v", snapshot)
			}
			if atomic.LoadUint64(&telemetry.Metrics.resumeSuccessTotal) != 1 || atomic.LoadUint64(&telemetry.Metrics.replayedEventsTotal) != 4 {
				t.Fatal("resume metrics not preserved")
			}
		})
	}
}

func TestReplayPaginationAndBoundary(t *testing.T) {
	f, store := newReplayRedis(t, 2005, nil)
	boundary, err := store.ReplayBoundary("room")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.append(2006, "later", "later", "Alice")
	f.mu.Unlock()
	for _, cursor := range []int64{1, 1500, 2005} {
		t.Run(fmt.Sprint(cursor), func(t *testing.T) {
			sequences := []int64{}
			err := store.EventsAfterSequence("room", cursor, boundary, func(e DistributedEvent) error { sequences = append(sequences, e.Sequence); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if len(sequences) != int(boundary.Sequence-cursor) {
				t.Fatalf("count=%d", len(sequences))
			}
			for i, n := range sequences {
				if n != cursor+int64(i)+1 {
					t.Fatalf("sequence[%d]=%d", i, n)
				}
			}
		})
	}
	if err := store.EventsAfterSequence("room", 2006, boundary, func(DistributedEvent) error { return nil }); err == nil {
		t.Fatal("accepted cursor beyond boundary")
	}
	f.mu.Lock()
	pages := f.pages
	f.mu.Unlock()
	if pages != 6 {
		t.Fatalf("expected 3 pages per nonempty replay, got %d", pages)
	}
}

func TestReplayRejectsMissingHistory(t *testing.T) {
	for _, missing := range []int{3, 5} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			f, store := newReplayRedis(t, 5, nil)
			boundary, err := store.ReplayBoundary("room")
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.events = append(f.events[:missing-1], f.events[missing:]...)
			f.mu.Unlock()
			err = store.EventsAfterSequence("room", 1, boundary, func(DistributedEvent) error { return nil })
			if !errors.Is(err, errReplayGap) {
				t.Fatalf("missing %d: %v", missing, err)
			}
		})
	}
}
