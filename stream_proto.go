package rmux

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type streamProto interface {
	tryRead(b []byte) (int, error)
	write(b []byte) (int, error)
	writeTo(w io.Writer) (int64, error)
	pushRecv(buf []byte, timer *time.Timer) bool
	update(consumed uint32, window uint32)
	recycleTokens() int
	hasBuffered() bool
}

type streamProtoV1 struct {
	s            *Stream
	bufferRing   bufferRing
	bufferLock   sync.Mutex
	recvBuffered int32
	recvLimit    int32
	recvTimeout  time.Duration
	chRecvSpace  chan struct{}
}

func newStreamProtoV1(s *Stream, sess *Session) *streamProtoV1 {
	return &streamProtoV1{
		s:           s,
		bufferRing:  newBufferRing(8),
		recvLimit:   int32(sess.config.MaxStreamBuffer),
		recvTimeout: sess.config.StreamReadTimeout,
		chRecvSpace: make(chan struct{}, 1),
	}
}

func (p *streamProtoV1) pushRecv(buf []byte, timer *time.Timer) bool {
	n := int32(len(buf))
	s := p.s
	if atomic.LoadInt32(&p.recvBuffered)+n <= p.recvLimit {
		p.bufferLock.Lock()
		p.bufferRing.push(buf, buf)
		p.bufferLock.Unlock()
		atomic.AddInt32(&p.recvBuffered, n)
		return true
	}
	var timeout <-chan time.Time
	if p.recvTimeout > 0 && timer != nil {
		stopTimer(timer)
		timer.Reset(p.recvTimeout)
		timeout = timer.C
		defer stopTimer(timer)
	}
	for atomic.LoadInt32(&p.recvBuffered)+n > p.recvLimit {
		select {
		case <-p.chRecvSpace:
		case <-timeout:
			return false
		case <-s.die:
			p.bufferLock.Lock()
			p.bufferRing.push(buf, buf)
			p.bufferLock.Unlock()
			atomic.AddInt32(&p.recvBuffered, n)
			return true
		case <-s.sess.die:
			p.bufferLock.Lock()
			p.bufferRing.push(buf, buf)
			p.bufferLock.Unlock()
			atomic.AddInt32(&p.recvBuffered, n)
			return true
		}
	}
	p.bufferLock.Lock()
	p.bufferRing.push(buf, buf)
	p.bufferLock.Unlock()
	atomic.AddInt32(&p.recvBuffered, n)
	return true
}

func (p *streamProtoV1) update(consumed uint32, window uint32) {}

func (p *streamProtoV1) recycleTokens() (n int) {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()
	for p.bufferRing.len() > 0 {
		buf, head, _ := p.bufferRing.pop()
		n += len(buf)
		defaultAllocator.Put(head)
	}
	return
}

func (p *streamProtoV1) hasBuffered() bool {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()
	return p.bufferRing.len() > 0
}

func (p *streamProtoV1) tryRead(b []byte) (n int, err error) {
	s := p.s
	if len(b) == 0 {
		return 0, nil
	}
	var recycled []byte
	p.bufferLock.Lock()
	n, recycled = p.bufferRing.consumeFront(b)
	p.bufferLock.Unlock()
	if recycled != nil {
		defaultAllocator.Put(recycled)
	}
	if n > 0 {
		atomic.AddInt32(&p.recvBuffered, -int32(n))
		select {
		case p.chRecvSpace <- struct{}{}:
		default:
		}
		return n, nil
	}
	select {
	case <-s.die:
		return 0, io.EOF
	default:
		return 0, ErrWouldBlock
	}
}

func (p *streamProtoV1) write(b []byte) (n int, err error) {
	s := p.s
	if len(b) == 0 {
		return 0, nil
	}
	if err := s.checkWriteClosed(); err != nil {
		return 0, err
	}
	var deadline <-chan time.Time
	if d, ok := s.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := getTimer(time.Until(d))
		defer putTimer(timer)
		deadline = timer.C
	}
	sent := 0
	frame := newFrame(1, cmdPSH, s.id)
	for len(b) > 0 {
		size := len(b)
		if size > s.frameSize {
			size = s.frameSize
		}
		frame.data = b[:size]
		n, err := s.sess.writeFrameInternal(frame, deadline, CLSDATA)
		sent += n
		if err != nil {
			return sent, err
		}
		b = b[size:]
	}
	return sent, nil
}

func (p *streamProtoV1) writeTo(w io.Writer) (n int64, err error) {
	s := p.s
	for {
		var buf []byte
		var head []byte
		p.bufferLock.Lock()
		if p.bufferRing.len() > 0 {
			buf, head, _ = p.bufferRing.pop()
		}
		p.bufferLock.Unlock()
		if buf != nil {
			nw, ew := w.Write(buf)
			atomic.AddInt32(&p.recvBuffered, -int32(len(buf)))
			select {
			case p.chRecvSpace <- struct{}{}:
			default:
			}
			defaultAllocator.Put(head)
			if nw > 0 {
				n += int64(nw)
			}
			if ew != nil {
				return n, ew
			}
		} else if ew := s.waitRead(); ew != nil {
			return n, ew
		}
	}
}

type streamProtoV2 struct {
	s                     *Stream
	bufferRing            bufferRing
	bufferLock            sync.Mutex
	numRead               uint32
	numWritten            uint32
	incr                  uint32
	peerConsumed          uint32
	peerWindow            uint32
	windowUpdateThreshold uint32
	chUpdate              chan struct{}
}

func newStreamProtoV2(s *Stream, sess *Session) *streamProtoV2 {
	return &streamProtoV2{
		s:                     s,
		bufferRing:            newBufferRing(8),
		peerWindow:            initialPeerWindow,
		chUpdate:              make(chan struct{}, 1),
		windowUpdateThreshold: uint32(sess.config.MaxStreamBuffer / 2),
	}
}

func (p *streamProtoV2) pushRecv(buf []byte, _ *time.Timer) bool {
	p.bufferLock.Lock()
	p.bufferRing.push(buf, buf)
	p.bufferLock.Unlock()
	return true
}

func (p *streamProtoV2) update(consumed uint32, window uint32) {
	atomic.StoreUint32(&p.peerConsumed, consumed)
	atomic.StoreUint32(&p.peerWindow, window)
	select {
	case p.chUpdate <- struct{}{}:
	default:
	}
}

func (p *streamProtoV2) recycleTokens() (n int) {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()
	for p.bufferRing.len() > 0 {
		buf, head, _ := p.bufferRing.pop()
		n += len(buf)
		defaultAllocator.Put(head)
	}
	return
}

func (p *streamProtoV2) hasBuffered() bool {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()
	return p.bufferRing.len() > 0
}

func (p *streamProtoV2) sendWindowUpdate(consumed uint32) error {
	s := p.s
	var timer *time.Timer
	var deadline <-chan time.Time
	if d, ok := s.readDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer = getTimer(time.Until(d))
		defer putTimer(timer)
		deadline = timer.C
	}
	frame := newFrame(2, cmdUPD, s.id)
	var hdr updHeader
	binary.LittleEndian.PutUint32(hdr[:], consumed)
	binary.LittleEndian.PutUint32(hdr[4:], uint32(s.sess.config.MaxStreamBuffer))
	frame.data = hdr[:]
	_, err := s.sess.writeFrameInternal(frame, deadline, CLSCTRL)
	return err
}

func (p *streamProtoV2) tryRead(b []byte) (n int, err error) {
	s := p.s
	if len(b) == 0 {
		return 0, nil
	}
	var notifyConsumed uint32
	var recycled []byte
	p.bufferLock.Lock()
	n, recycled = p.bufferRing.consumeFront(b)
	p.numRead += uint32(n)
	p.incr += uint32(n)
	if p.incr >= p.windowUpdateThreshold || p.numRead == uint32(n) {
		notifyConsumed = p.numRead
		p.incr = 0
	}
	p.bufferLock.Unlock()
	if recycled != nil {
		defaultAllocator.Put(recycled)
	}
	if n > 0 {
		if notifyConsumed > 0 {
			return n, p.sendWindowUpdate(notifyConsumed)
		}
		return n, nil
	}
	select {
	case <-s.die:
		return 0, io.EOF
	default:
		return 0, ErrWouldBlock
	}
}

func (p *streamProtoV2) write(b []byte) (n int, err error) {
	s := p.s
	if len(b) == 0 {
		return 0, nil
	}
	if err := s.checkWriteClosed(); err != nil {
		return 0, err
	}
	sent := 0
	frame := newFrame(2, cmdPSH, s.id)
	var deadlineTimer *time.Timer
	defer func() {
		stopTimer(deadlineTimer)
	}()
	for {
		deadline := (<-chan time.Time)(nil)
		if d, ok := s.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
			dur := time.Until(d)
			if dur < 0 {
				dur = 0
			}
			if deadlineTimer == nil {
				deadlineTimer = time.NewTimer(dur)
			} else {
				stopTimer(deadlineTimer)
				deadlineTimer.Reset(dur)
			}
			deadline = deadlineTimer.C
		} else if deadlineTimer != nil {
			stopTimer(deadlineTimer)
			deadlineTimer = nil
		}
		inflight := int32(atomic.LoadUint32(&p.numWritten) - atomic.LoadUint32(&p.peerConsumed))
		if inflight < 0 {
			return 0, ErrConsumed
		}
		win := int32(atomic.LoadUint32(&p.peerWindow)) - inflight
		if win > 0 {
			n := len(b)
			if n > int(win) {
				n = int(win)
			}
			bts := b[:n]
			for len(bts) > 0 {
				size := len(bts)
				if size > s.frameSize {
					size = s.frameSize
				}
				frame.data = bts[:size]
				nw, err := s.sess.writeFrameInternal(frame, deadline, CLSDATA)
				atomic.AddUint32(&p.numWritten, uint32(size))
				sent += nw
				if err != nil {
					return sent, err
				}
				bts = bts[size:]
			}
			b = b[n:]
		}
		if len(b) <= 0 {
			return sent, nil
		}
		select {
		case <-s.chWriterWakeup:
		case <-s.chWriteClosed:
			return sent, io.ErrClosedPipe
		case <-s.die:
			return sent, io.ErrClosedPipe
		case <-deadline:
			return sent, ErrTimeout
		case <-s.sess.chSocketWriteError:
			return sent, s.sess.socketWriteError.Load().(error)
		case <-s.sess.chSocketReadError:
			return sent, s.sess.socketReadError.Load().(error)
		case <-p.chUpdate:
			continue
		}
	}
}

func (p *streamProtoV2) writeTo(w io.Writer) (n int64, err error) {
	s := p.s
	for {
		var notifyConsumed uint32
		var buf []byte
		var head []byte
		p.bufferLock.Lock()
		if p.bufferRing.len() > 0 {
			buf, head, _ = p.bufferRing.pop()
		}
		var bufLen uint32
		if buf != nil {
			bufLen = uint32(len(buf))
		}
		p.numRead += bufLen
		p.incr += bufLen
		if p.incr >= p.windowUpdateThreshold || p.numRead == bufLen {
			notifyConsumed = p.numRead
			p.incr = 0
		}
		p.bufferLock.Unlock()
		if buf != nil {
			nw, ew := w.Write(buf)
			defaultAllocator.Put(head)
			if nw > 0 {
				n += int64(nw)
			}
			if ew != nil {
				return n, ew
			}
			if notifyConsumed > 0 {
				if err := p.sendWindowUpdate(notifyConsumed); err != nil {
					return n, err
				}
			}
		} else if ew := s.waitRead(); ew != nil {
			return n, ew
		}
	}
}
