package rmux

import (
	"sync"
	"sync/atomic"
)

func _itimediff(later, earlier uint32) int32 {
	return (int32)(later - earlier)
}

type shaperHeap []writeRequest

func (h shaperHeap) Len() int { return len(h) }

func (h shaperHeap) Less(i, j int) bool {
	if h[i].class != h[j].class {
		return h[i].class < h[j].class
	}
	return _itimediff(h[j].seq, h[i].seq) > 0
}

func (h shaperHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *shaperHeap) Push(x any)   { *h = append(*h, x.(writeRequest)) }

func (h *shaperHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = writeRequest{}
	*h = old[0 : n-1]
	return x
}

func (h shaperHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h shaperHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

func (h *shaperHeap) pushReq(req writeRequest) {
	*h = append(*h, req)
	h.up(len(*h) - 1)
}

func (h *shaperHeap) popReq() writeRequest {
	old := *h
	n := len(old) - 1
	old.Swap(0, n)
	old[:n].down(0, n)
	req := old[n]
	old[n] = writeRequest{}
	*h = old[:n]
	return req
}

type rrNode struct {
	sid  uint32
	prev *rrNode
	next *rrNode
}

type rrList struct {
	head *rrNode
	size int
	free *rrNode
}

func (l *rrList) Len() int { return l.size }

func (l *rrList) pushBack(sid uint32) *rrNode {
	n := l.free
	if n != nil {
		l.free = n.next
		n.next = nil
		n.prev = nil
	} else {
		n = &rrNode{}
	}
	n.sid = sid
	if l.head == nil {
		n.prev = n
		n.next = n
		l.head = n
	} else {
		tail := l.head.prev
		tail.next = n
		n.prev = tail
		n.next = l.head
		l.head.prev = n
	}
	l.size++
	return n
}

func (l *rrList) remove(n *rrNode) {
	if l.size == 1 {
		l.head = nil
	} else {
		n.prev.next = n.next
		n.next.prev = n.prev
		if l.head == n {
			l.head = n.next
		}
	}
	l.size--
	n.prev = nil
	n.sid = 0
	n.next = l.free
	l.free = n
}

type shaperQueue struct {
	count   int64
	streams map[uint32]*shaperHeap
	rrList  rrList
	next    *rrNode
	mu      sync.Mutex
}

var shaperHeapPool = sync.Pool{
	New: func() any {
		h := make(shaperHeap, 0, 16)
		return &h
	},
}

func NewShaperQueue() *shaperQueue {
	return &shaperQueue{
		streams: make(map[uint32]*shaperHeap),
	}
}

func (sq *shaperQueue) Push(req writeRequest) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sid := req.sid
	if _, ok := sq.streams[sid]; !ok {
		h := shaperHeapPool.Get().(*shaperHeap)
		*h = (*h)[:0]
		sq.streams[sid] = h
		elem := sq.rrList.pushBack(sid)
		if sq.next == nil {
			sq.next = elem
		}
	}
	h := sq.streams[sid]
	h.pushReq(req)
	atomic.AddInt64(&sq.count, 1)
}

func (sq *shaperQueue) Pop() (req writeRequest, ok bool) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.next == nil || atomic.LoadInt64(&sq.count) == 0 {
		return writeRequest{}, false
	}
	start := sq.next
	current := start
	for {
		sid := current.sid
		h := sq.streams[sid]
		if h.Len() > 0 {
			req := h.popReq()
			atomic.AddInt64(&sq.count, -1)
			next := current.next
			if h.Len() == 0 {
				delete(sq.streams, sid)
				shaperHeapPool.Put(h)
				if sq.rrList.Len() == 1 {
					sq.rrList.remove(current)
					sq.next = nil
				} else {
					if next == current {
						next = current.next
					}
					sq.rrList.remove(current)
					sq.next = next
				}
			} else {
				sq.next = next
			}
			return req, true
		}
		current = current.next
		if current == start {
			break
		}
	}
	return writeRequest{}, false
}

func (sq *shaperQueue) IsEmpty() bool {
	return atomic.LoadInt64(&sq.count) == 0
}

func (sq *shaperQueue) Len() int {
	return int(atomic.LoadInt64(&sq.count))
}
