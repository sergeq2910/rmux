package rmux

type ringEntry struct {
	buf  []byte
	head []byte
}

type bufferRing struct {
	entries []ringEntry
	head    int
	tail    int
	size    int
	mask    int
	initCap int
}

func newBufferRing(capacity int) bufferRing {
	if capacity < 1 {
		capacity = 1
	}
	c := 1
	for c < capacity {
		c <<= 1
	}
	return bufferRing{initCap: c}
}

func (r *bufferRing) len() int {
	return r.size
}

func (r *bufferRing) grow() {
	newCap := len(r.entries) * 2
	if newCap < r.initCap {
		newCap = r.initCap
	}
	if newCap < 1 {
		newCap = 1
	}
	newEntries := make([]ringEntry, newCap)
	for i := 0; i < r.size; i++ {
		newEntries[i] = r.entries[(r.head+i)&r.mask]
	}
	r.entries = newEntries
	r.head = 0
	r.tail = r.size
	r.mask = newCap - 1
}

func (r *bufferRing) push(buf []byte, head []byte) {
	if r.size == len(r.entries) {
		r.grow()
	}
	r.entries[r.tail] = ringEntry{buf: buf, head: head}
	r.tail = (r.tail + 1) & r.mask
	r.size++
}

func (r *bufferRing) pop() (buf []byte, head []byte, ok bool) {
	if r.size == 0 {
		return nil, nil, false
	}
	e := r.entries[r.head]
	r.entries[r.head] = ringEntry{}
	r.head = (r.head + 1) & r.mask
	r.size--
	if r.size == 0 {
		r.tail = r.head
	}
	return e.buf, e.head, true
}

func (r *bufferRing) consumeFront(b []byte) (n int, recycled []byte) {
	if r.size == 0 {
		return 0, nil
	}
	e := &r.entries[r.head]
	n = copy(b, e.buf)
	e.buf = e.buf[n:]
	if len(e.buf) == 0 {
		recycled = e.head
		*e = ringEntry{}
		r.head = (r.head + 1) & r.mask
		r.size--
		if r.size == 0 {
			r.tail = r.head
		}
	}
	return n, recycled
}
