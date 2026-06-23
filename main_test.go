package main

import (
	"testing"
	"time"
)

func TestBrokerSetAndGetMessage(t *testing.T) {
	b := NewBroker()

	b.SetMessage("queue", "first")

	got, err := b.GetMessage("queue", time.Second)
	if err != nil {
		t.Fatalf("GetMessage returned error: %v", err)
	}

	if got != "first" {
		t.Fatalf("got %q, want %q", got, "first")
	}
}

func TestBrokerGetMessageFIFO(t *testing.T) {
	b := NewBroker()

	b.SetMessage("queue", "first")
	b.SetMessage("queue", "second")

	got, err := b.GetMessage("queue", time.Second)
	if err != nil {
		t.Fatalf("GetMessage returned error: %v", err)
	}
	if got != "first" {
		t.Fatalf("first GetMessage got %q, want %q", got, "first")
	}

	got, err = b.GetMessage("queue", time.Second)
	if err != nil {
		t.Fatalf("GetMessage returned error: %v", err)
	}
	if got != "second" {
		t.Fatalf("second GetMessage got %q, want %q", got, "second")
	}
}

func TestBrokerGetMessageWaitsUntilMessageAppears(t *testing.T) {
	b := NewBroker()
	gotCh := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		got, err := b.GetMessage("queue", 0)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- got
	}()

	select {
	case got := <-gotCh:
		t.Fatalf("GetMessage returned before message appeared: %q", got)
	case err := <-errCh:
		t.Fatalf("GetMessage returned error before message appeared: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	b.SetMessage("queue", "message")

	select {
	case err := <-errCh:
		t.Fatalf("GetMessage returned error: %v", err)
	case got := <-gotCh:
		if got != "message" {
			t.Fatalf("got %q, want %q", got, "message")
		}
	case <-time.After(time.Second):
		t.Fatal("GetMessage did not return after message appeared")
	}
}

func TestBrokerGetMessageTimeout(t *testing.T) {
	b := NewBroker()

	start := time.Now()
	got, err := b.GetMessage("queue", 30*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("GetMessage got nil error, want timeout error with result %q", got)
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("GetMessage returned too early: %s", elapsed)
	}
}

func TestBrokerGetBeforeQueueExistsThenSetMessage(t *testing.T) {
	b := NewBroker()
	gotCh := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		got, err := b.GetMessage("new-queue", time.Second)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- got
	}()

	time.Sleep(20 * time.Millisecond)
	b.SetMessage("new-queue", "created")

	select {
	case err := <-errCh:
		t.Fatalf("GetMessage returned error: %v", err)
	case got := <-gotCh:
		if got != "created" {
			t.Fatalf("got %q, want %q", got, "created")
		}
	case <-time.After(time.Second):
		t.Fatal("GetMessage did not return after queue was created")
	}
}

func TestBrokerQueuesAreIndependent(t *testing.T) {
	b := NewBroker()

	b.SetMessage("first-queue", "first")
	b.SetMessage("second-queue", "second")

	got, err := b.GetMessage("second-queue", time.Second)
	if err != nil {
		t.Fatalf("GetMessage returned error: %v", err)
	}
	if got != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}

	got, err = b.GetMessage("first-queue", time.Second)
	if err != nil {
		t.Fatalf("GetMessage returned error: %v", err)
	}
	if got != "first" {
		t.Fatalf("got %q, want %q", got, "first")
	}
}
