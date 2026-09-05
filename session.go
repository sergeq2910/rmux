package rmux

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
)

const (
	defaultAcceptBacklog = 1024
	maxShaperSize        = 1024
	openCloseTimeout     = 30 * time.Second
	coalesceThreshold    = 1024
)

var resultChanPool = sync.Pool{
	New: func() any {
		return make(chan writeResult, 1)
	},
}

type CLASSID int32

const (
	CLSCTRL CLASSID = iota
	CLSDATA
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Timeout() bool   { return true }

var (
	ErrInvalidProtocol           = errors.New("invalid protocol")
	ErrConsumed                  = errors.New("peer consumed more than sent")
	ErrGoAway                    = errors.New("stream id overflows, should start a new connection")
	ErrTimeout         net.Error = &timeoutError{}
	ErrWouldBlock                = errors.New("operation would block on IO")
)

type writeRequest struct {
	data   []byte
	result chan writeResult
	sid    uint32
	seq    uint32
	class  CLASSID
	ver    byte
	cmd    byte
}

type writeResult struct {
	n   int
	err error
}

type Session struct {
	conn io.ReadWriteCloser

	config           *Config
	goAway           int32
	nextStreamID     uint32
	nextStreamIDLock sync.Mutex

	streams    map[uint32]*Stream
	streamLock sync.Mutex

	die     chan struct{}
	dieOnce sync.Once
	closed  int32

	socketReadError      atomic.Value
	socketWriteError     atomic.Value
	chSocketReadError    chan struct{}
	chSocketWriteError   chan struct{}
	socketReadErrorOnce  sync.Once
	socketWriteErrorOnce sync.Once

	protoError     atomic.Value
	chProtoError   chan struct{}
	protoErrorOnce sync.Once

	chAccepts chan *Stream

	sessionIsActive int32
	acceptDeadline  atomic.Value

	requestID uint32
	shaper    chan writeRequest
	sq        *shaperQueue

	lastActivity int64
	probeMu      sync.Mutex
	chPong       chan struct{}
}

func newSession(config *Config, conn io.ReadWriteCloser, client bool) *Session {
	s := new(Session)
	s.die = make(chan struct{})
	s.conn = conn
	s.config = config
	s.streams = make(map[uint32]*Stream)
	s.chAccepts = make(chan *Stream, defaultAcceptBacklog)
	s.shaper = make(chan writeRequest, maxShaperSize)
	s.chSocketReadError = make(chan struct{})
	s.chSocketWriteError = make(chan struct{})
	s.chProtoError = make(chan struct{})
	s.sq = NewShaperQueue()
	s.chPong = make(chan struct{}, 1)
	s.lastActivity = time.Now().UnixNano()
	if client {
		s.nextStreamID = 1
	} else {
		s.nextStreamID = 0
	}
	go s.recvLoop()
	go s.sendLoop()
	if !config.KeepAliveDisabled {
		go s.keepalive()
	}
	return s
}

func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, io.ErrClosedPipe
	}
	if err := s.probeIfIdle(); err != nil {
		return nil, err
	}
	s.nextStreamIDLock.Lock()
	if s.goAway > 0 {
		s.nextStreamIDLock.Unlock()
		return nil, ErrGoAway
	}
	if s.nextStreamID+2 < s.nextStreamID {
		s.goAway = 1
		s.nextStreamIDLock.Unlock()
		return nil, ErrGoAway
	}
	s.nextStreamID += 2
	sid := s.nextStreamID
	s.nextStreamIDLock.Unlock()
	stream := newStream(sid, s.config.MaxFrameSize, s)
	s.streamLock.Lock()
	s.streams[sid] = stream
	s.streamLock.Unlock()
	if _, err := s.writeControlFrame(newFrame(byte(s.config.Version), cmdSYN, sid)); err != nil {
		s.streamClosed(sid)
		return nil, err
	}
	select {
	case <-s.chSocketReadError:
		s.streamClosed(sid)
		return nil, s.socketReadError.Load().(error)
	case <-s.chSocketWriteError:
		s.streamClosed(sid)
		return nil, s.socketWriteError.Load().(error)
	case <-s.die:
		s.streamClosed(sid)
		return nil, io.ErrClosedPipe
	default:
		return stream, nil
	}
}

func (s *Session) Open() (io.ReadWriteCloser, error) {
	return s.OpenStream()
}

func (s *Session) AcceptStream() (*Stream, error) {
	var deadline <-chan time.Time
	if d, ok := s.acceptDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := getTimer(time.Until(d))
		defer putTimer(timer)
		deadline = timer.C
	}
	select {
	case stream := <-s.chAccepts:
		return stream, nil
	case <-deadline:
		return nil, ErrTimeout
	case <-s.chSocketReadError:
		return nil, s.socketReadError.Load().(error)
	case <-s.chProtoError:
		return nil, s.protoError.Load().(error)
	case <-s.die:
		return nil, io.ErrClosedPipe
	}
}

func (s *Session) Accept() (io.ReadWriteCloser, error) {
	return s.AcceptStream()
}

func (s *Session) Close() error {
	var once bool
	s.dieOnce.Do(func() {
		atomic.StoreInt32(&s.closed, 1)
		close(s.die)
		once = true
	})
	if !once {
		return io.ErrClosedPipe
	}
	s.streamLock.Lock()
	for k, stream := range s.streams {
		stream.sessionClose()
		stream.recycleTokens()
		delete(s.streams, k)
	}
	s.streamLock.Unlock()
	return s.conn.Close()
}

func (s *Session) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) != 0
}

func (s *Session) NumStreams() int {
	if s.IsClosed() {
		return 0
	}
	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	return len(s.streams)
}

func (s *Session) SetDeadline(t time.Time) error {
	s.acceptDeadline.Store(t)
	return nil
}

func (s *Session) LocalAddr() net.Addr {
	if ts, ok := s.conn.(interface {
		LocalAddr() net.Addr
	}); ok {
		return ts.LocalAddr()
	}
	return nil
}

func (s *Session) RemoteAddr() net.Addr {
	if ts, ok := s.conn.(interface {
		RemoteAddr() net.Addr
	}); ok {
		return ts.RemoteAddr()
	}
	return nil
}

func (s *Session) notifyReadError(err error) {
	s.socketReadErrorOnce.Do(func() {
		s.socketReadError.Store(err)
		close(s.chSocketReadError)
		s.Close()
	})
}

func (s *Session) notifyWriteError(err error) {
	s.socketWriteErrorOnce.Do(func() {
		s.socketWriteError.Store(err)
		close(s.chSocketWriteError)
		s.Close()
	})
}

func (s *Session) notifyProtoError(err error) {
	s.protoErrorOnce.Do(func() {
		s.protoError.Store(err)
		close(s.chProtoError)
		s.Close()
	})
}

func (s *Session) markActivity() {
	atomic.StoreInt64(&s.lastActivity, time.Now().UnixNano())
}

func (s *Session) idleFor() time.Duration {
	last := atomic.LoadInt64(&s.lastActivity)
	return time.Since(time.Unix(0, last))
}

func (s *Session) probeIfIdle() error {
	probeAfter := s.config.IdleProbeAfter
	if probeAfter <= 0 {
		return nil
	}
	if s.idleFor() < probeAfter {
		return nil
	}
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.idleFor() < probeAfter {
		return nil
	}
	select {
	case <-s.chPong:
	default:
	}
	timeout := s.config.IdleProbeTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timer := getTimer(timeout)
	defer putTimer(timer)
	if _, err := s.writeFrameInternal(newFrame(byte(s.config.Version), cmdPING, 0), timer.C, CLSCTRL); err != nil {
		s.Close()
		return err
	}
	select {
	case <-s.chPong:
		s.markActivity()
		return nil
	case <-s.chSocketReadError:
		s.Close()
		return s.socketReadError.Load().(error)
	case <-s.chProtoError:
		s.Close()
		return s.protoError.Load().(error)
	case <-s.die:
		return io.ErrClosedPipe
	case <-timer.C:
		s.Close()
		return ErrTimeout
	}
}

func (s *Session) streamClosed(sid uint32) {
	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	stream, ok := s.streams[sid]
	if !ok {
		return
	}
	stream.recycleTokens()
	delete(s.streams, sid)
}

func (s *Session) recvLoop() {
	var hdr rawHeader
	var updHdr updHeader
	var recvSpaceTimer *time.Timer
	if s.config.StreamReadTimeout > 0 {
		recvSpaceTimer = time.NewTimer(s.config.StreamReadTimeout)
		stopTimer(recvSpaceTimer)
	}
	for {
		_, err := io.ReadFull(s.conn, hdr[:])
		if err != nil {
			s.notifyReadError(err)
			return
		}
		atomic.StoreInt32(&s.sessionIsActive, 1)
		s.markActivity()
		if hdr.Version() != byte(s.config.Version) {
			s.notifyProtoError(ErrInvalidProtocol)
			return
		}
		sid := hdr.StreamID()
		switch hdr.Cmd() {
		case cmdNOP:
			if hdr.Length() != 0 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
		case cmdPING:
			if hdr.Length() != 0 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			go func() {
				timer := getTimer(openCloseTimeout)
				defer putTimer(timer)
				s.writeFrameInternal(newFrame(byte(s.config.Version), cmdPONG, 0), timer.C, CLSCTRL)
			}()
		case cmdPONG:
			if hdr.Length() != 0 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			select {
			case s.chPong <- struct{}{}:
			default:
			}
		case cmdSYN:
			if hdr.Length() != 0 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			var accepted *Stream
			s.streamLock.Lock()
			if _, ok := s.streams[sid]; !ok {
				stream := newStream(sid, s.config.MaxFrameSize, s)
				s.streams[sid] = stream
				accepted = stream
			}
			s.streamLock.Unlock()
			if accepted != nil {
				select {
				case s.chAccepts <- accepted:
				case <-s.die:
				}
			}
		case cmdFIN:
			if hdr.Length() != 0 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			s.streamLock.Lock()
			st := s.streams[sid]
			s.streamLock.Unlock()
			if st != nil {
				st.fin()
			}
		case cmdPSH:
			if hdr.Length() == 0 {
				continue
			}
			pNewbuf := defaultAllocator.Get(int(hdr.Length()))
			_, err := io.ReadFull(s.conn, pNewbuf)
			if err != nil {
				s.notifyReadError(err)
				defaultAllocator.Put(pNewbuf)
				return
			}
			s.streamLock.Lock()
			stream, ok := s.streams[sid]
			s.streamLock.Unlock()
			if !ok {
				defaultAllocator.Put(pNewbuf)
				break
			}
			if !stream.pushRecv(pNewbuf, recvSpaceTimer) {
				s.streamClosed(sid)
				stream.sessionClose()
				defaultAllocator.Put(pNewbuf)
				break
			}
			stream.wakeupReader()
		case cmdUPD:
			if s.config.Version != 2 {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			if hdr.Length() != szCmdUPD {
				s.notifyProtoError(ErrInvalidProtocol)
				return
			}
			_, err := io.ReadFull(s.conn, updHdr[:])
			if err != nil {
				s.notifyReadError(err)
				return
			}
			s.streamLock.Lock()
			st := s.streams[sid]
			s.streamLock.Unlock()
			if st != nil {
				st.proto.update(updHdr.Consumed(), updHdr.Window())
			}
		default:
			s.notifyProtoError(ErrInvalidProtocol)
			return
		}
	}
}

func (s *Session) keepalive() {
	tickerPing := time.NewTicker(s.config.KeepAliveInterval)
	tickerTimeout := time.NewTicker(s.config.KeepAliveTimeout)
	defer tickerPing.Stop()
	defer tickerTimeout.Stop()
	for {
		select {
		case <-tickerPing.C:
			s.writeFrameInternal(newFrame(byte(s.config.Version), cmdNOP, 0), tickerPing.C, CLSCTRL)
		case <-tickerTimeout.C:
			if !atomic.CompareAndSwapInt32(&s.sessionIsActive, 1, 0) {
				s.Close()
				return
			}
		case <-s.die:
			return
		}
	}
}

func (s *Session) sendLoop() {
	var n int
	var err error
	hdr := make([]byte, headerSize)
	var writeFrame func(data []byte) (int, error)
	if bw, ok := bufio.CreateVectorisedWriter(s.conn); ok {
		hdrBuffer := buf.As(hdr)
		dataBuffer := buf.As(nil)
		bufVec := []*buf.Buffer{hdrBuffer, dataBuffer}
		coalesce := make([]byte, headerSize+coalesceThreshold)
		writeFrame = func(data []byte) (int, error) {
			if len(data) <= coalesceThreshold {
				copy(coalesce, hdr)
				copy(coalesce[headerSize:], data)
				if _, ew := s.conn.Write(coalesce[:headerSize+len(data)]); ew != nil {
					return 0, ew
				}
				return len(data), nil
			}
			*hdrBuffer = *buf.As(hdr)
			*dataBuffer = *buf.As(data)
			if ew := bw.WriteVectorised(bufVec); ew != nil {
				return 0, ew
			}
			return len(data), nil
		}
	} else {
		body := make([]byte, (1<<16)+headerSize)
		writeFrame = func(data []byte) (int, error) {
			copy(body, hdr)
			copy(body[headerSize:], data)
			nw, ew := s.conn.Write(body[:headerSize+len(data)])
			nw -= headerSize
			if nw < 0 {
				nw = 0
			}
			return nw, ew
		}
	}
	for {
		select {
		case <-s.die:
			return
		case r := <-s.shaper:
			s.sq.Push(r)
			for len(s.shaper) > 0 && s.sq.Len() < maxShaperSize {
				select {
				case r := <-s.shaper:
					s.sq.Push(r)
				default:
				}
			}
			for {
				request, ok := s.sq.Pop()
				if !ok {
					break
				}
				hdr[0] = request.ver
				hdr[1] = request.cmd
				binary.LittleEndian.PutUint16(hdr[2:], uint16(len(request.data)))
				binary.LittleEndian.PutUint32(hdr[4:], request.sid)
				n, err = writeFrame(request.data)
				request.result <- writeResult{
					n:   n,
					err: err,
				}
				if err != nil {
					s.notifyWriteError(err)
					return
				}
			}
		}
	}
}

func (s *Session) writeControlFrame(f Frame) (n int, err error) {
	timer := getTimer(openCloseTimeout)
	defer putTimer(timer)
	return s.writeFrameInternal(f, timer.C, CLSCTRL)
}

func (s *Session) writeFrameInternal(f Frame, deadline <-chan time.Time, class CLASSID) (int, error) {
	resultCh := resultChanPool.Get().(chan writeResult)
	req := writeRequest{
		class:  class,
		data:   f.data,
		sid:    f.sid,
		ver:    f.ver,
		cmd:    f.cmd,
		seq:    atomic.AddUint32(&s.requestID, 1),
		result: resultCh,
	}
	select {
	case s.shaper <- req:
	default:
		select {
		case s.shaper <- req:
		case <-s.die:
			resultChanPool.Put(resultCh)
			return 0, io.ErrClosedPipe
		case <-s.chSocketWriteError:
			resultChanPool.Put(resultCh)
			return 0, s.socketWriteError.Load().(error)
		case <-deadline:
			resultChanPool.Put(resultCh)
			return 0, ErrTimeout
		}
	}
	select {
	case result := <-resultCh:
		resultChanPool.Put(resultCh)
		return result.n, result.err
	case <-s.die:
		return 0, io.ErrClosedPipe
	case <-s.chSocketWriteError:
		return 0, s.socketWriteError.Load().(error)
	case <-deadline:
		return 0, ErrTimeout
	}
}
