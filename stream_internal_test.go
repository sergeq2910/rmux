package rmux

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func newUnitTestStream() *Stream {
	cfg := DefaultConfig()
	sess := &Session{
		config:             cfg,
		streams:            make(map[uint32]*Stream),
		chSocketReadError:  make(chan struct{}),
		chSocketWriteError: make(chan struct{}),
		chProtoError:       make(chan struct{}),
	}
	st := newStream(1, cfg.MaxFrameSize, sess)
	sess.streams[st.id] = st
	return st
}

func TestStreamWaitReadTimeout(t *testing.T) {
	s := newUnitTestStream()
	s.readDeadline.Store(time.Now().Add(20 * time.Millisecond))
	if err := s.waitRead(); err != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestStreamWaitReadFinWithBufferedData(t *testing.T) {
	s := newUnitTestStream()
	buf := []byte("abc")
	p := s.proto.(*streamProtoV1)
	p.bufferLock.Lock()
	p.bufferRing.push(buf, buf)
	p.bufferLock.Unlock()

	s.fin()
	if err := s.waitRead(); err != nil {
		t.Fatalf("expected nil after fin with buffered data, got %v", err)
	}

	readBuf := make([]byte, 3)
	n, err := s.proto.tryRead(readBuf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 bytes, got %d", n)
	}
	if !bytes.Equal(readBuf, []byte("abc")) {
		t.Fatalf("read mismatch: %q", readBuf)
	}
}

func TestStreamRecycleTokens(t *testing.T) {
	s := newUnitTestStream()
	p := s.proto.(*streamProtoV1)
	b1 := []byte("hello")
	b2 := []byte("world!")
	p.bufferLock.Lock()
	p.bufferRing.push(b1, b1)
	p.bufferRing.push(b2, b2)
	p.bufferLock.Unlock()

	n := s.recycleTokens()
	if n != len(b1)+len(b2) {
		t.Fatalf("unexpected recycled bytes: %d", n)
	}
	if s.proto.hasBuffered() {
		t.Fatal("expected empty buffer ring")
	}
}

func TestStreamUpdateNotifiesWriter(t *testing.T) {
	s := newUnitTestStreamV2()
	p := s.proto.(*streamProtoV2)
	s.proto.update(7, 9)

	if got := atomic.LoadUint32(&p.peerConsumed); got != 7 {
		t.Fatalf("peerConsumed mismatch: %d", got)
	}
	if got := atomic.LoadUint32(&p.peerWindow); got != 9 {
		t.Fatalf("peerWindow mismatch: %d", got)
	}

	select {
	case <-p.chUpdate:
		// ok
	default:
		t.Fatal("expected chUpdate notification")
	}
}

func TestStreamSetDeadlineWakesUp(t *testing.T) {
	s := newUnitTestStream()
	if err := s.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline failed: %v", err)
	}

	select {
	case <-s.chReaderWakeup:
		// ok
	default:
		t.Fatal("expected reader wakeup")
	}

	select {
	case <-s.chWriterWakeup:
		// ok
	default:
		t.Fatal("expected writer wakeup")
	}
}

func TestStreamWaitReadClosed(t *testing.T) {
	s := newUnitTestStream()
	close(s.die)
	if err := s.waitRead(); err != io.ErrClosedPipe {
		t.Fatalf("expected io.ErrClosedPipe, got %v", err)
	}
}

func TestSendWindowUpdateTimeout(t *testing.T) {
	s := newUnitTestStreamV2()
	s.readDeadline.Store(time.Now().Add(-time.Second))

	if err := s.proto.(*streamProtoV2).sendWindowUpdate(1); err != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestStopTimer(t *testing.T) {
	stopTimer(nil)

	timer := time.NewTimer(time.Nanosecond)
	<-timer.C
	stopTimer(timer)

	active := time.NewTimer(time.Second)
	stopTimer(active)
}

func TestNewBufferRingMinCapacity(t *testing.T) {
	r := newBufferRing(0)
	if len(r.entries) != 0 {
		t.Fatalf("expected lazy allocation, got len %d", len(r.entries))
	}
	b := []byte{1}
	r.push(b, b)
	if len(r.entries) != 1 {
		t.Fatalf("expected capacity 1 after first push, got %d", len(r.entries))
	}
}

func newUnitTestStreamV2() *Stream {
	cfg := DefaultConfig()
	cfg.Version = 2
	sess := &Session{
		config:             cfg,
		streams:            make(map[uint32]*Stream),
		chSocketReadError:  make(chan struct{}),
		chSocketWriteError: make(chan struct{}),
		chProtoError:       make(chan struct{}),
		die:                make(chan struct{}),
	}
	st := newStream(1, cfg.MaxFrameSize, sess)
	sess.streams[st.id] = st
	return st
}

func TestWriteV2ClosedPipe(t *testing.T) {
	s := newUnitTestStreamV2()
	close(s.chWriteClosed)

	if _, err := s.proto.write([]byte("x")); err != io.ErrClosedPipe {
		t.Fatalf("expected io.ErrClosedPipe, got %v", err)
	}
}

func TestWriteV2ConsumedError(t *testing.T) {
	s := newUnitTestStreamV2()
	p := s.proto.(*streamProtoV2)
	atomic.StoreUint32(&p.peerConsumed, 10)
	atomic.StoreUint32(&p.numWritten, 0)
	atomic.StoreUint32(&p.peerWindow, 0)

	if _, err := s.proto.write([]byte("x")); err != ErrConsumed {
		t.Fatalf("expected ErrConsumed, got %v", err)
	}
}

func TestWriteV2TimeoutWhenWindowZero(t *testing.T) {
	s := newUnitTestStreamV2()
	atomic.StoreUint32(&s.proto.(*streamProtoV2).peerWindow, 0)
	s.writeDeadline.Store(time.Now().Add(-time.Second))

	if _, err := s.proto.write([]byte("data")); err != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}
