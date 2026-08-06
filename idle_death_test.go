package rmux

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// blackholeConn models a silently dead TCP connection (e.g. an idle session
// dropped by a NAT/firewall without RST/FIN): writes are accepted and
// discarded, reads block forever, and no error is ever reported.
type blackholeConn struct {
	closeOnce sync.Once
	closed    chan struct{}
	writeFail bool // if true, Write returns an error instead of blackholing
}

func newBlackholeConn() *blackholeConn {
	return &blackholeConn{closed: make(chan struct{})}
}

func (c *blackholeConn) Read(b []byte) (int, error) {
	<-c.closed // block until explicitly closed; otherwise hang forever
	return 0, io.EOF
}

func (c *blackholeConn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	if c.writeFail {
		return 0, io.ErrClosedPipe
	}
	return len(b), nil // accepted and discarded
}

func (c *blackholeConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *blackholeConn) LocalAddr() net.Addr  { return &net.TCPAddr{} }
func (c *blackholeConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (c *blackholeConn) SetDeadline(time.Time) error      { return nil }
func (c *blackholeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *blackholeConn) SetWriteDeadline(time.Time) error { return nil }

// idleProbeConfig returns a keepalive-disabled config with an aggressive lazy
// idle probe, matching the intended sing-mux setup: no periodic pings, but a
// one-shot NOP ping/pong probe on OpenStream when the session has been idle
// (no read and no write) for IdleProbeAfter.
func idleProbeConfig(version int) *Config {
	c := DefaultConfig()
	c.Version = version
	c.KeepAliveDisabled = true
	c.IdleProbeAfter = 50 * time.Millisecond
	c.IdleProbeTimeout = 500 * time.Millisecond
	return c
}

// TestIdleSilentDeathReconnects reproduces the real-world bug: a session whose
// transport has silently died while idle. When the user returns and tries to
// use it, the round-trip must NOT hang for ~a minute (OS TCP timeout) — the
// lazy idle probe has to surface the failure quickly so the caller can dial a
// fresh connection.
func TestIdleSilentDeathReconnects(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			bh := newBlackholeConn()
			session, err := Client(bh, idleProbeConfig(version))
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			defer bh.Close()

			// let the session go idle past IdleProbeAfter
			time.Sleep(150 * time.Millisecond)

			// Simulate "user returns after long idle and uses the session".
			result := make(chan error, 1)
			go func() {
				stream, oerr := session.OpenStream()
				if oerr != nil {
					result <- oerr
					return
				}
				if _, oerr = stream.Write([]byte("ping")); oerr != nil {
					result <- oerr
					return
				}
				buf := make([]byte, 4)
				_, oerr = io.ReadFull(stream, buf)
				result <- oerr
			}()

			// A healthy reconnect path must surface the dead connection within
			// a couple seconds, not wait for the ~1 minute OS TCP timeout.
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("expected an error from the silently-dead session, got nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("round-trip on silently-dead idle session hung " +
					"(idle probe did not surface the failure)")
			}

			if !session.IsClosed() {
				t.Fatal("session should be closed after a failed idle probe")
			}
		})
	}
}


// TestIdleProbeKeepsHealthySession verifies the lazy probe does not kill a
// healthy but idle session: over a live TCP pair the NOP ping/pong round-trips,
// OpenStream succeeds, and the session stays open.
func TestIdleProbeKeepsHealthySession(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			c1, c2, err := getTCPConnectionPair()
			if err != nil {
				t.Fatal(err)
			}

			cfg := idleProbeConfig(version)
			client, err := Client(c1, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := Server(c2, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()

			// drain anything the server accepts so it does not block
			go func() {
				for {
					st, aerr := server.AcceptStream()
					if aerr != nil {
						return
					}
					go func(st *Stream) {
						io.Copy(io.Discard, st)
						st.Close()
					}(st)
				}
			}()

			// go idle past IdleProbeAfter, then open: probe must succeed
			time.Sleep(150 * time.Millisecond)

			done := make(chan error, 1)
			go func() {
				st, oerr := client.OpenStream()
				if oerr != nil {
					done <- oerr
					return
				}
				st.Close()
				done <- nil
			}()

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("OpenStream on healthy idle session failed: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("OpenStream on healthy idle session hung")
			}

			if client.IsClosed() {
				t.Fatal("healthy session must not be closed by the idle probe")
			}
		})
	}
}

// keepAliveConfig returns a config with aggressive keepalive so a silently dead
// transport is detected quickly (matching how sing-mux configures rmux).
func keepAliveConfig(version int) *Config {
	c := DefaultConfig()
	c.Version = version
	c.KeepAliveDisabled = false
	c.KeepAliveInterval = 200 * time.Millisecond
	c.KeepAliveTimeout = 600 * time.Millisecond
	return c
}

// TestIdleSilentDeathDetectedByKeepAlive verifies the fix: with keepalive
// enabled, a session whose transport has silently died is closed by the
// keepalive watchdog within KeepAliveTimeout, so a blocked round-trip surfaces
// an error quickly instead of hanging until the OS TCP timeout.
func TestIdleSilentDeathDetectedByKeepAlive(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			bh := newBlackholeConn()
			session, err := Client(bh, keepAliveConfig(version))
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			defer bh.Close()

			result := make(chan error, 1)
			go func() {
				stream, oerr := session.OpenStream()
				if oerr != nil {
					result <- oerr
					return
				}
				if _, oerr = stream.Write([]byte("ping")); oerr != nil {
					result <- oerr
					return
				}
				buf := make([]byte, 4)
				_, oerr = io.ReadFull(stream, buf)
				result <- oerr
			}()

			select {
			case err := <-result:
				if err == nil {
					t.Fatal("expected an error from the dead session, got nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("keepalive did not surface the dead session within 3s")
			}

			if !session.IsClosed() {
				t.Fatal("session should be closed by keepalive watchdog")
			}
		})
	}
}
