package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Extend the existing RESP fixture without changing document/replay semantics.
func (f *replayRedis) presenceCommand(a []string) any {
	if a[0] == "XREAD" {
		key := strings.TrimSuffix(a[len(a)-2], "-events")
		cursor := streamNumber(a[len(a)-1])
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		for {
			f.mu.Lock()
			if f.presenceChanged == nil {
				f.presenceChanged = make(chan struct{})
			}
			revision := f.presenceRevisions[key]
			changed := f.presenceChanged
			f.mu.Unlock()
			if revision > cursor {
				entries := []any{}
				for next := cursor + 1; next <= revision && len(entries) < 100; next++ {
					entries = append(entries, []any{fmt.Sprintf("%d-0", next), []any{"revision", strconv.FormatInt(next, 10)}})
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.presenceRecords == nil {
		f.presenceRecords = make(map[string]map[string]string)
		f.presenceRevisions = make(map[string]int64)
	}
	if f.presenceChanged == nil {
		f.presenceChanged = make(chan struct{})
	}
	key := a[3]
	if f.presenceRecords[key] == nil {
		f.presenceRecords[key] = make(map[string]string)
	}
	records := f.presenceRecords[key]
	revision := f.presenceRevisions[key]
	switch a[1] {
	case presenceSnapshotScript:
		values := []any{}
		for _, record := range records {
			values = append(values, record)
		}
		return []any{revision, values}
	case joinPresenceScript:
		if _, exists := records[a[6]]; exists {
			return revision
		}
		records[a[6]] = a[7]
	case leavePresenceScript:
		if _, exists := records[a[6]]; !exists {
			return revision
		}
		delete(records, a[6])
	default:
		return fmt.Errorf("unknown presence script")
	}
	f.presenceRevisions[key]++
	close(f.presenceChanged)
	f.presenceChanged = make(chan struct{})
	return revision + 1
}

func presenceTestStore(t *testing.T) (*RedisStore, string) {
	t.Helper()
	addr := os.Getenv("LIVECOLLAB_TEST_PRESENCE_REDIS_ADDR")
	if addr == "" {
		_, store := newReplayRedis(t, 0, nil)
		return store, "room"
	}
	store := NewRedisStore(addr)
	id, err := newConnectionID()
	if err != nil {
		t.Fatal(err)
	}
	roomID := "presence-test-" + id
	if err := store.CreateRoom(roomID); err != nil {
		t.Fatal(err)
	}
	// Only delete keys belonging to this randomly generated test room.
	t.Cleanup(func() {
		_, err := store.do("DEL", roomExistsKey(roomID), roomContentKey(roomID), roomVersionKey(roomID), roomSequenceKey(roomID), roomStreamKey(roomID), roomPresenceKey(roomID), roomPresenceRevisionKey(roomID), roomPresenceStreamKey(roomID))
		if err != nil {
			t.Errorf("clean test room: %v", err)
		}
	})
	return store, roomID
}

type presenceBackend struct {
	manager *RoomManager
	server  *httptest.Server
}

func newPresenceBackend(t *testing.T, store *RedisStore, instance string) *presenceBackend {
	t.Helper()
	config := Config{InstanceID: instance, StaleWritePolicy: StaleWriteReject}
	telemetry := NewTelemetry(config)
	manager := NewDistributedRoomManager(store, instance, nil, telemetry)
	var handlers sync.WaitGroup
	handler := NewApplication(manager, config, nil, telemetry).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { manager.Close(); server.Close(); handlers.Wait() })
	return &presenceBackend{manager: manager, server: server}
}

type presenceFrame struct {
	Message
	PresenceSnapshot
	UsersField json.RawMessage `json:"users"`
}

type presenceSocket struct {
	conn   *websocket.Conn
	latest PresenceSnapshot
	frames []presenceFrame
}

func connectPresence(t *testing.T, backend *presenceBackend, roomID, username, clientID string) *presenceSocket {
	t.Helper()
	query := url.Values{"roomId": {roomID}, "username": {username}, "clientId": {clientID}}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(backend.server.URL, "http")+"/ws?"+query.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &presenceSocket{conn: conn, latest: PresenceSnapshot{Revision: -1}}
}

func (s *presenceSocket) read(t *testing.T) presenceFrame {
	t.Helper()
	_ = s.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var frame presenceFrame
	if err := s.conn.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type == "error" {
		t.Fatalf("server error: %+v", frame.Message)
	}
	if frame.Type == "content_update" {
		t.Fatal("presence test unexpectedly needed/received a document edit")
	}
	if frame.Type == "presence_update" {
		if frame.Revision < s.latest.Revision {
			t.Fatalf("backend delivered stale presence: %d after %d", frame.Revision, s.latest.Revision)
		}
		s.latest = frame.PresenceSnapshot
	}
	s.frames = append(s.frames, frame)
	return frame
}

func (s *presenceSocket) wait(t *testing.T, revision int64) PresenceSnapshot {
	t.Helper()
	for s.latest.Revision < revision {
		s.read(t)
	}
	if s.latest.Revision != revision {
		t.Fatalf("presence revision=%d, want %d", s.latest.Revision, revision)
	}
	return s.latest
}

func assertPresence(t *testing.T, snapshot PresenceSnapshot, names ...string) {
	t.Helper()
	actual := []string{}
	ids := map[string]bool{}
	for _, p := range snapshot.Participants {
		if len(p.ConnectionID) != 32 || ids[p.ConnectionID] || p.InstanceID == "" {
			t.Fatalf("invalid connection identity: %+v", p)
		}
		ids[p.ConnectionID] = true
		actual = append(actual, p.Username)
	}
	sort.Strings(actual)
	sort.Strings(names)
	if !reflect.DeepEqual(actual, names) {
		t.Fatalf("participants=%v, want %v", actual, names)
	}
}

func assertDocumentUnchanged(t *testing.T, store *RedisStore, roomID string) {
	t.Helper()
	snapshot, exists, err := store.GetSnapshot(roomID)
	if err != nil || !exists || snapshot.Sequence != 0 || snapshot.Version != 0 || snapshot.Content != "" {
		t.Fatalf("presence changed document state: %+v exists=%t err=%v", snapshot, exists, err)
	}
}

func TestPresenceAcrossBackends(t *testing.T) {
	store, roomID := presenceTestStore(t)
	a, b := newPresenceBackend(t, store, "a"), newPresenceBackend(t, store, "b")
	alice := connectPresence(t, a, roomID, "Alice", "alice-client")
	assertPresence(t, alice.wait(t, 1), "Alice")
	bob := connectPresence(t, b, roomID, "Bob", "bob-client")
	left, right := alice.wait(t, 2), bob.wait(t, 2)
	assertPresence(t, left, "Alice", "Bob")
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("backends disagree: %+v / %+v", left, right)
	}
	for _, socket := range []*presenceSocket{alice, bob} {
		for _, frame := range socket.frames {
			if frame.Type == "room_state" && len(frame.UsersField) != 0 {
				t.Fatal("distributed room_state contains local users")
			}
		}
	}
	assertDocumentUnchanged(t, store, roomID)
}

func TestPresenceReconnectOnDifferentBackend(t *testing.T) {
	store, roomID := presenceTestStore(t)
	a, b := newPresenceBackend(t, store, "a"), newPresenceBackend(t, store, "b")
	observer := connectPresence(t, b, roomID, "Alice", "alice")
	observer.wait(t, 1)
	old := connectPresence(t, a, roomID, "Bob", "same-client")
	before := old.wait(t, 2)
	observer.wait(t, 2)
	oldID := ""
	for _, p := range before.Participants {
		if p.Username == "Bob" {
			oldID = p.ConnectionID
		}
	}
	// Hold the old socket open until the replacement is registered on B.
	replacement := connectPresence(t, b, roomID, "Bob", "same-client")
	overlap := replacement.wait(t, 3)
	old.wait(t, 3)
	observer.wait(t, 3)
	assertPresence(t, overlap, "Alice", "Bob", "Bob")
	_ = old.conn.Close()
	final := replacement.wait(t, 4)
	if !reflect.DeepEqual(final, observer.wait(t, 4)) {
		t.Fatal("observers did not converge after reconnect")
	}
	assertPresence(t, final, "Alice", "Bob")
	for _, p := range final.Participants {
		if p.ConnectionID == oldID {
			t.Fatal("old connection survived cleanup")
		}
	}
	if err := store.LeavePresence(roomID, oldID); err != nil {
		t.Fatal(err)
	}
	repeated, err := store.GetPresence(roomID)
	if err != nil || !reflect.DeepEqual(final, repeated) {
		t.Fatalf("repeated old cleanup changed replacement: %+v %v", repeated, err)
	}
	assertDocumentUnchanged(t, store, roomID)
}

func TestPresenceSameUsernameAndOneTabCloses(t *testing.T) {
	store, roomID := presenceTestStore(t)
	a, b := newPresenceBackend(t, store, "a"), newPresenceBackend(t, store, "b")
	observer := connectPresence(t, a, roomID, "Observer", "observer")
	observer.wait(t, 1)
	first := connectPresence(t, a, roomID, "Smriti", "shared-client")
	first.wait(t, 2)
	observer.wait(t, 2)
	second := connectPresence(t, b, roomID, "Smriti", "shared-client")
	both := second.wait(t, 3)
	first.wait(t, 3)
	observer.wait(t, 3)
	assertPresence(t, both, "Observer", "Smriti", "Smriti")
	remainingID := ""
	for _, p := range both.Participants {
		if p.Username == "Smriti" && p.ClientID != "shared-client" {
			t.Fatal("clientId not preserved")
		}
		if p.Username == "Smriti" && p.InstanceID == "b" {
			remainingID = p.ConnectionID
		}
	}
	_ = first.conn.Close()
	final := second.wait(t, 4)
	if !reflect.DeepEqual(final, observer.wait(t, 4)) {
		t.Fatal("observers disagree after one tab closes")
	}
	assertPresence(t, final, "Observer", "Smriti")
	found := false
	for _, p := range final.Participants {
		if p.ConnectionID == remainingID {
			found = true
		}
	}
	if !found {
		t.Fatal("closing first tab removed second tab")
	}
	assertDocumentUnchanged(t, store, roomID)
}

func TestPresenceDelayedSnapshotCannotRegress(t *testing.T) {
	store, roomID := presenceTestStore(t)
	a, b := newPresenceBackend(t, store, "a"), newPresenceBackend(t, store, "b")
	alice := connectPresence(t, a, roomID, "Alice", "alice")
	alice.wait(t, 1)
	older, err := store.GetPresence(roomID)
	if err != nil {
		t.Fatal(err)
	}
	room, _ := a.manager.GetRoom(roomID)
	client := room.clientsSnapshot()[0]
	paused, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	go func() { close(paused); <-release; done <- client.writePresence(older) }()
	awaitSignal(t, paused)
	bob := connectPresence(t, b, roomID, "Bob", "bob")
	newer := alice.wait(t, 2)
	bob.wait(t, 2)
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delayed snapshot did not finish")
	}
	// The typing response is an on-wire barrier after the delayed delivery attempt.
	if err := alice.conn.WriteJSON(Message{Type: "typing"}); err != nil {
		t.Fatal(err)
	}
	for {
		if alice.read(t).Type == "typing" {
			break
		}
	}
	if !reflect.DeepEqual(alice.latest, newer) {
		t.Fatal("older snapshot replaced newer presence")
	}
	client.writeMu.Lock()
	revision := client.presenceRevision
	client.writeMu.Unlock()
	if revision != 2 {
		t.Fatalf("backend presence revision regressed to %d", revision)
	}
}

func TestPresenceRoomStateDoesNotResetGlobalList(t *testing.T) {
	store, roomID := presenceTestStore(t)
	a, b := newPresenceBackend(t, store, "a"), newPresenceBackend(t, store, "b")
	alice := connectPresence(t, a, roomID, "Alice", "alice")
	alice.wait(t, 1)
	bob := connectPresence(t, b, roomID, "Bob", "bob")
	before := alice.wait(t, 2)
	bob.wait(t, 2)
	if err := alice.conn.WriteJSON(Message{Type: "content_update", OperationID: "stale", BaseVersion: int64Pointer(99), Content: "must not commit"}); err != nil {
		t.Fatal(err)
	}
	for {
		frame := alice.read(t)
		if frame.Type == "room_state" && frame.Reason == "resync_after_conflict" {
			if len(frame.UsersField) != 0 {
				t.Fatal("conflict resync leaked backend-local users")
			}
			break
		}
	}
	if !reflect.DeepEqual(before, alice.latest) {
		t.Fatal("document resync changed presence revision/list")
	}
	assertDocumentUnchanged(t, store, roomID)
}

func TestPresenceLocalFallback(t *testing.T) {
	backend := newPresenceBackend(t, nil, "local")
	room, err := backend.manager.CreateRoom()
	if err != nil {
		t.Fatal(err)
	}
	alice := connectPresence(t, backend, room.ID, "Smriti", "same")
	alice.wait(t, 1)
	bob := connectPresence(t, backend, room.ID, "Smriti", "same")
	assertPresence(t, bob.wait(t, 2), "Smriti", "Smriti")
	alice.wait(t, 2)
	_ = alice.conn.Close()
	assertPresence(t, bob.wait(t, 3), "Smriti")
}
