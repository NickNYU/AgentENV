package main

// TODO(#341, B6 — author review): this helper and its test can be a
// nil-able chan struct{} in main (if wait { ready = make(chan struct{}) });
// discovery would take `ready chan<- struct{}` and nil-check before close,
// instead of a variadic used as one optional channel (yingdi-shan).
//
// discoveryReadyFor returns the ready-channel wiring for kubernetes
// discovery. A channel exists only when startup waits for the first sync
// (leader election on kubernetes discovery): discovery closes it once its
// initial sync completes. Every other mode must hand discovery no channel
// at all — discovery owns the close, and a caller-side close panics as
// "close of closed channel" once the post-sync close runs (#341 review,
// guozy18).
func discoveryReadyFor(waitForDiscoverySync bool) (chan struct{}, []chan<- struct{}) {
	if !waitForDiscoverySync {
		return nil, nil
	}
	ready := make(chan struct{})
	return ready, []chan<- struct{}{ready}
}
