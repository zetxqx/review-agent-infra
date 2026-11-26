package sandbox

import "time"

const (
	kitFinalizeGrace = 450 * time.Second
	execWaitDelay    = 10 * time.Second
)

func kitExecTimeout(wallClockSeconds int) time.Duration {
	return time.Duration(wallClockSeconds)*time.Second + kitFinalizeGrace
}
