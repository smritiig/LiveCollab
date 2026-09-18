package main

import (
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func leaseJoin(t *testing.T, s *RedisStore, id, name string) {
	t.Helper()
	if err := s.JoinPresence("room", Participant{ConnectionID: id, ClientID: id, Username: name, InstanceID: "crashed"}); err != nil {
		t.Fatal(err)
	}
}
func leaseAdvance(f *replayRedis, d time.Duration) {
	f.mu.Lock()
	f.presenceNow += d.Milliseconds()
	f.mu.Unlock()
}
func leaseState(f *replayRedis, id string) (bool, int64, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, exists := f.presenceRecords[roomPresenceKey("room")][id]
	return exists, f.presenceExpiry[roomPresenceKey("room")][id], f.presenceRevisions[roomPresenceKey("room")]
}
func TestPresenceLeaseRenewal(t *testing.T) {
	f, s := newReplayRedis(t, 0, nil)
	leaseJoin(t, s, "alice", "Alice")
	leaseAdvance(f, 25*time.Second)
	ok, err := s.RenewPresence("room", "alice", defaultPresenceLease)
	if err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	exists, expiry, revision := leaseState(f, "alice")
	if !exists || expiry != 55000 || revision != 1 {
		t.Fatalf("state %v %d %d", exists, expiry, revision)
	}
	if err := s.LeavePresence("room", "alice"); err != nil {
		t.Fatal(err)
	}
	ok, err = s.RenewPresence("room", "alice", defaultPresenceLease)
	if err != nil || ok {
		t.Fatalf("resurrection: %v %v", ok, err)
	}
	exists, expiry, revision = leaseState(f, "alice")
	if exists || expiry != 0 || revision != 2 {
		t.Fatalf("removed state %v %d %d", exists, expiry, revision)
	}
	if err := s.LeavePresence("room", "alice"); err != nil {
		t.Fatal(err)
	}
	_, _, revision = leaseState(f, "alice")
	if revision != 2 {
		t.Fatal("repeated removal advanced revision")
	}
}
func TestPresenceLeaseConcurrentSweepers(t *testing.T) {
	f, s := newReplayRedis(t, 0, nil)
	leaseJoin(t, s, "bob", "Bob")
	leaseAdvance(f, 31*time.Second)
	ok, err := s.RenewPresence("room", "bob", defaultPresenceLease)
	if err != nil || ok {
		t.Fatalf("expired renewed: %v %v", ok, err)
	}
	start := make(chan struct{})
	results := make(chan int64, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := s.PrunePresence("room")
			if err != nil {
				t.Error(err)
			}
			results <- n
		}()
	}
	close(start)
	wg.Wait()
	if (<-results)+(<-results) != 1 {
		t.Fatal("cleanup did not remove exactly once")
	}
	exists, expiry, revision := leaseState(f, "bob")
	if exists || expiry != 0 || revision != 2 {
		t.Fatalf("state %v %d %d", exists, expiry, revision)
	}
}
func TestPresenceLeaseSnapshotAndRestart(t *testing.T) {
	f, s := newReplayRedis(t, 0, nil)
	leaseJoin(t, s, "old", "Old")
	leaseAdvance(f, 31*time.Second)
	snapshot, err := s.GetPresence("room")
	if err != nil || len(snapshot.Participants) != 0 || snapshot.Revision != 2 {
		t.Fatalf("snapshot %+v %v", snapshot, err)
	}
	leaseJoin(t, s, "orphan", "Orphan")
	leaseAdvance(f, 31*time.Second)
	m := NewDistributedRoomManager(s, "restart", nil, nil)
	defer m.Close()
	if err := m.sweepPresenceOnce(); err != nil {
		t.Fatal(err)
	}
	exists, expiry, revision := leaseState(f, "orphan")
	if exists || expiry != 0 || revision != 4 {
		t.Fatalf("restart state %v %d %d", exists, expiry, revision)
	}
}
func TestPresenceLeaseCrashConverges(t *testing.T) {
	for _, name := range []string{"Bob", "Alice"} {
		t.Run(name, func(t *testing.T) {
			f, s := newReplayRedis(t, 0, nil)
			a, b := newPresenceBackend(t, s, "a"), newPresenceBackend(t, s, "b")
			alice := connectPresence(t, a, "room", "Alice", "alice")
			alice.wait(t, 1)
			observer := connectPresence(t, b, "room", "Observer", "observer")
			observer.wait(t, 2)
			alice.wait(t, 2)
			leaseJoin(t, s, "dead", name)
			alice.wait(t, 3)
			observer.wait(t, 3)
			leaseAdvance(f, 25*time.Second)
			for _, p := range alice.latest.Participants {
				if p.ConnectionID != "dead" {
					ok, err := s.RenewPresence("room", p.ConnectionID, defaultPresenceLease)
					if err != nil || !ok {
						t.Fatalf("renew %v %v", ok, err)
					}
				}
			}
			leaseAdvance(f, 6*time.Second)
			if err := a.manager.sweepPresenceOnce(); err != nil {
				t.Fatal(err)
			}
			left, right := alice.wait(t, 4), observer.wait(t, 4)
			if !reflect.DeepEqual(left, right) {
				t.Fatal("backends disagree")
			}
			assertPresence(t, left, "Alice", "Observer")
			if n, err := s.PrunePresence("room"); err != nil || n != 0 {
				t.Fatalf("repeat prune %d %v", n, err)
			}
			assertDocumentUnchanged(t, s, "room")
		})
	}
}
func TestPresenceLeaseWebSocketLiveness(t *testing.T) {
	for _, pong := range []bool{true, false} {
		name := "silent"
		if pong {
			name = "pong"
		}
		t.Run(name, func(t *testing.T) {
			f, s := newReplayRedis(t, 0, nil)
			renewed := make(chan struct{}, 1)
			f.mu.Lock()
			f.presenceRenewed = renewed
			f.mu.Unlock()
			backend := newPresenceBackend(t, s, "a", PresenceTiming{HeartbeatInterval: 20 * time.Millisecond, LeaseDuration: 200 * time.Millisecond})
			socket := connectPresence(t, backend, "room", "Alice", "alice")
			snapshot := socket.wait(t, 1)
			if !pong {
				socket.conn.SetPingHandler(func(string) error { return nil })
			}
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				for {
					if _, _, err := socket.conn.ReadMessage(); err != nil {
						return
					}
				}
			}()
			if pong {
				awaitSignal(t, renewed)
				socket.conn.Close()
				awaitSignal(t, closed)
			} else {
				awaitSignal(t, closed)
				select {
				case <-renewed:
					t.Fatal("silent socket renewed")
				default:
				}
			}
			changes, err := s.ReadPresenceChanges("room", "1-0")
			if err != nil || changes == "1-0" {
				t.Fatalf("disconnect notification %v %v", changes, err)
			}
			exists, _, revision := leaseState(f, snapshot.Participants[0].ConnectionID)
			if exists || revision != 2 {
				t.Fatalf("disconnect state %v %d", exists, revision)
			}
		})
	}
}

// Exercise actual Lua/Redis TIME and notification counts when Redis is available.
func TestPresenceLeaseRedisScripts(t *testing.T) {
	if os.Getenv("LIVECOLLAB_TEST_PRESENCE_REDIS_ADDR") == "" {
		t.Skip("set LIVECOLLAB_TEST_PRESENCE_REDIS_ADDR for real Redis")
	}
	s, room := presenceTestStore(t)
	p := Participant{ConnectionID: "lease-test", ClientID: "client", Username: "Alice", InstanceID: "a"}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	value := func(args ...string) any { t.Helper(); v, err := s.do(args...); must(err); return v }
	count := func() int64 {
		t.Helper()
		n, err := redisInt64(value("XLEN", roomPresenceStreamKey(room)))
		must(err)
		return n
	}
	must(s.JoinPresence(room, p))
	original := redisString(value("ZSCORE", roomPresenceExpiryKey(room), p.ConnectionID))
	ok, err := s.RenewPresence(room, p.ConnectionID, 2*defaultPresenceLease)
	must(err)
	if !ok {
		t.Fatal("renew rejected")
	}
	extended := redisString(value("ZSCORE", roomPresenceExpiryKey(room), p.ConnectionID))
	if extended <= original || count() != 1 {
		t.Fatalf("renew expiry %s -> %s, events %d", original, extended, count())
	}
	must(s.LeavePresence(room, p.ConnectionID))
	ok, err = s.RenewPresence(room, p.ConnectionID, defaultPresenceLease)
	must(err)
	if ok || value("ZSCORE", roomPresenceExpiryKey(room), p.ConnectionID) != nil || count() != 2 {
		t.Fatal("removed membership resurrected")
	}
	must(s.JoinPresence(room, p))
	value("ZADD", roomPresenceExpiryKey(room), "0", p.ConnectionID)
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan int64, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := s.PrunePresence(room)
			if err != nil {
				t.Error(err)
			}
			results <- n
		}()
	}
	close(start)
	wg.Wait()
	if (<-results)+(<-results) != 1 || count() != 4 {
		t.Fatalf("concurrent cleanup events=%d", count())
	}
	must(s.JoinPresence(room, p))
	value("ZADD", roomPresenceExpiryKey(room), "0", p.ConnectionID)
	snapshot, err := s.GetPresence(room)
	must(err)
	if len(snapshot.Participants) != 0 || snapshot.Revision != 6 || count() != 6 {
		t.Fatalf("snapshot %+v events=%d", snapshot, count())
	}
}

// Opt-in validation against the actual Compose backends and Nginx gateway.
func TestPresenceLeaseRealTopology(t *testing.T) {
	if os.Getenv("LIVECOLLAB_TEST_PRESENCE_TOPOLOGY") != "1" {
		t.Skip("set LIVECOLLAB_TEST_PRESENCE_TOPOLOGY=1 with Compose running")
	}
	if os.Getenv("LIVECOLLAB_TEST_PRESENCE_REDIS_ADDR") == "" {
		t.Fatal("real topology needs real Redis")
	}
	s, room := presenceTestStore(t)
	backend := func(port string) *presenceBackend {
		return &presenceBackend{server: &httptest.Server{URL: "http://127.0.0.1:" + port}}
	}
	alice := connectPresence(t, backend("8081"), room, "Alice", "alice")
	alice.wait(t, 1)
	bob := connectPresence(t, backend("8082"), room, "Bob", "bob")
	bob.wait(t, 2)
	alice.wait(t, 2)
	gateway := connectPresence(t, backend("8080"), room, "Observer", "observer")
	gateway.wait(t, 3)
	bob.wait(t, 3)
	before := alice.wait(t, 3)
	var bobID string
	for _, p := range before.Participants {
		if p.Username == "Bob" {
			bobID = p.ConnectionID
			if p.InstanceID != "backend-b" {
				t.Fatal("Bob not on B")
			}
		}
		if p.Username == "Alice" && p.InstanceID != "backend-a" {
			t.Fatal("Alice not on A")
		}
	}
	// Deterministically expire only this test connection. Production sweepers,
	// not a test-side prune/snapshot, must emit the next notification.
	if _, err := s.do("ZADD", roomPresenceExpiryKey(room), "0", bobID); err != nil {
		t.Fatal(err)
	}
	left, right, throughGateway := alice.wait(t, 4), bob.wait(t, 4), gateway.wait(t, 4)
	if !reflect.DeepEqual(left, right) || !reflect.DeepEqual(left, throughGateway) {
		t.Fatal("real topology did not converge")
	}
	assertPresence(t, left, "Alice", "Observer")
	assertDocumentUnchanged(t, s, room)
	// Close each socket before deleting the randomly generated test keys.
	alice.conn.Close()
	bob.conn.Close()
	gateway.conn.Close()
	cursor, err := s.ReadPresenceChanges(room, "0-0")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("graceful cleanup timed out")
		}
		snapshot, err := s.GetPresence(room)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Participants) == 0 {
			break
		}
		cursor, err = s.ReadPresenceChanges(room, cursor)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPresenceLeaseSnapshotKeepsActive(t *testing.T) {
	f, s := newReplayRedis(t, 0, nil)
	leaseJoin(t, s, "expired", "Same")
	leaseAdvance(f, 20*time.Second)
	leaseJoin(t, s, "active", "Same")
	leaseAdvance(f, 11*time.Second)
	snapshot, err := s.GetPresence("room")
	if err != nil || snapshot.Revision != 3 || len(snapshot.Participants) != 1 || snapshot.Participants[0].ConnectionID != "active" {
		t.Fatalf("snapshot %+v %v", snapshot, err)
	}
	again, err := s.GetPresence("room")
	if err != nil || !reflect.DeepEqual(snapshot, again) {
		t.Fatalf("unchanged snapshot %+v %v", again, err)
	}
}
