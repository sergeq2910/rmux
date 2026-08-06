package rmux

import (
	"sync"
	"time"
)

var timerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(time.Hour)
		if !t.Stop() {
			<-t.C
		}
		return t
	},
}

func getTimer(d time.Duration) *time.Timer {
	t := timerPool.Get().(*time.Timer)
	t.Reset(d)
	return t
}

func putTimer(t *time.Timer) {
	stopTimer(t)
	timerPool.Put(t)
}
