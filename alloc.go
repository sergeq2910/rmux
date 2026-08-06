package rmux

import (
	"github.com/sagernet/sing/common/buf"
)

var defaultAllocator = (*Allocator)(nil)

type Allocator struct {
}

func (alloc *Allocator) Get(size int) []byte {
	return buf.DefaultAllocator.Get(size)
}

func (alloc *Allocator) Put(buffer []byte) error {
	return buf.DefaultAllocator.Put(buffer)
}
