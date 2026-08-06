package rmux

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Stream struct {
	sess  *Session
	proto streamProto

	frameSize int

	chReaderWakeup chan struct{}
	chWriterWakeup chan struct{}

	die           chan struct{}
	chFinEvent    chan struct{}
	chWriteClosed chan struct{}

	readDeadline  atomic.Value
	writeDeadline atomic.Value

	dieOnce         sync.Once
	finEventOnce    sync.Once
	writeClosedOnce sync.Once

	id uint32
}

func newStream(id uint32, frameSize int, sess *Session) *Stream {
	s := new(Stream)
	s.chReaderWakeup = make(chan struct{}, 1)
	s.chWriterWakeup = make(chan struct{}, 1)
	s.frameSize = frameSize
	s.sess = sess
	s.die = make(chan struct{})
	s.chFinEvent = make(chan struct{})
	s.chWriteClosed = make(chan struct{})
	if sess.config.Version == 2 {
		s.proto = newStreamProtoV2(s, sess)
	} else {
		s.proto = newStreamProtoV1(s, sess)
	}
	s.id = id
	return s
}

func (s *Stream) Read(b []byte) (n int, err error) {
	for {
		n, err = s.proto.tryRead(b)
		if err != ErrWouldBlock {
			return n, err
		}
		if ew := s.waitRead(); ew != nil {
			return 0, ew
		}
	}
}

func (s *Stream) WriteTo(w io.Writer) (n int64, err error) {
	return s.proto.writeTo(w)
}

func (s *Stream) Write(b []byte) (n int, err error) {
	return s.proto.write(b)
}

func (s *Stream) CloseWrite() error {
	var once bool
	s.writeClosedOnce.Do(func() {
		close(s.chWriteClosed)
		once = true
	})
	if !once {
		return io.ErrClosedPipe
	}
	f := newFrame(byte(s.sess.config.Version), cmdFIN, s.id)
	timer := getTimer(openCloseTimeout)
	defer putTimer(timer)
	_, err := s.sess.writeFrameInternal(f, timer.C, CLSDATA)
	s.tryHalfCloseCleanup()
	return err
}

func (s *Stream) Close() error {
	var once bool
	s.dieOnce.Do(func() {
		close(s.die)
		once = true
	})
	if !once {
		return io.ErrClosedPipe
	}
	s.writeClosedOnce.Do(func() {
		close(s.chWriteClosed)
	})
	f := newFrame(byte(s.sess.config.Version), cmdFIN, s.id)
	timer := getTimer(openCloseTimeout)
	defer putTimer(timer)
	_, err := s.sess.writeFrameInternal(f, timer.C, CLSDATA)
	s.sess.streamClosed(s.id)
	return err
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	s.wakeupReader()
	return nil
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	s.wakeupWriter()
	return nil
}

func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

func (s *Stream) LocalAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		LocalAddr() net.Addr
	}); ok {
		return ts.LocalAddr()
	}
	return nil
}

func (s *Stream) RemoteAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		RemoteAddr() net.Addr
	}); ok {
		return ts.RemoteAddr()
	}
	return nil
}

func (s *Stream) waitRead() error {
	var timer *time.Timer
	var deadline <-chan time.Time
	if d, ok := s.readDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer = getTimer(time.Until(d))
		defer putTimer(timer)
		deadline = timer.C
	}
	select {
	case <-s.chReaderWakeup:
		return nil
	case <-s.chFinEvent:
		if s.proto.hasBuffered() {
			return nil
		}
		return io.EOF
	case <-s.sess.chSocketReadError:
		return s.sess.socketReadError.Load().(error)
	case <-s.sess.chSocketWriteError:
		return s.sess.socketWriteError.Load().(error)
	case <-s.sess.chProtoError:
		return s.sess.protoError.Load().(error)
	case <-deadline:
		return ErrTimeout
	case <-s.die:
		return io.ErrClosedPipe
	}
}

func (s *Stream) checkWriteClosed() error {
	select {
	case <-s.chWriteClosed:
		return io.ErrClosedPipe
	case <-s.die:
		return io.ErrClosedPipe
	default:
		return nil
	}
}

func (s *Stream) pushRecv(buf []byte, timer *time.Timer) bool {
	return s.proto.pushRecv(buf, timer)
}

func (s *Stream) recycleTokens() (n int) {
	return s.proto.recycleTokens()
}

func (s *Stream) wakeupReader() {
	select {
	case s.chReaderWakeup <- struct{}{}:
	default:
	}
}

func (s *Stream) wakeupWriter() {
	select {
	case s.chWriterWakeup <- struct{}{}:
	default:
	}
}

func (s *Stream) fin() {
	s.finEventOnce.Do(func() {
		close(s.chFinEvent)
	})
	s.tryHalfCloseCleanup()
}

func (s *Stream) sessionClose() { s.dieOnce.Do(func() { close(s.die) }) }

func (s *Stream) tryHalfCloseCleanup() {
	select {
	case <-s.chFinEvent:
	default:
		return
	}
	select {
	case <-s.chWriteClosed:
	default:
		return
	}
	s.dieOnce.Do(func() {
		close(s.die)
	})
	s.sess.streamClosed(s.id)
}

func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
