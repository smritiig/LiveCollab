package main

import "testing"

func TestApplyContentUpdateRejectsStaleWrite(t *testing.T) {
	room := &Room{Content: "Start", Version: 1}

	first := room.ApplyContentUpdate("Start [Alice]", 1, StaleWriteReject)
	if !first.Accepted || first.ServerVersion != 2 {
		t.Fatalf("expected first update to be accepted at version 2: %+v", first)
	}

	stale := room.ApplyContentUpdate("Start [Bob]", 1, StaleWriteReject)
	if stale.Accepted {
		t.Fatalf("expected stale update to be rejected: %+v", stale)
	}
	if stale.Reason != "stale_base_version" {
		t.Fatalf("unexpected rejection reason: %q", stale.Reason)
	}
	if snapshot := room.GetSnapshot(); snapshot.Content != "Start [Alice]" || snapshot.Version != 2 {
		t.Fatalf("stale update changed room state: %+v", snapshot)
	}
}

func TestApplyContentUpdateCanReproduceOriginalUnsafeBehavior(t *testing.T) {
	room := &Room{Content: "Start", Version: 1}
	_ = room.ApplyContentUpdate("Start [Alice]", 1, StaleWriteAccept)

	stale := room.ApplyContentUpdate("Start [Bob]", 1, StaleWriteAccept)
	if !stale.Accepted || !stale.Stale {
		t.Fatalf("expected unsafe policy to accept a stale update: %+v", stale)
	}
	if snapshot := room.GetSnapshot(); snapshot.Content != "Start [Bob]" || snapshot.Version != 3 {
		t.Fatalf("expected stale write to overwrite Alice: %+v", snapshot)
	}
}
