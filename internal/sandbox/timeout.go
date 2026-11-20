package sandbox

import "time"

const kitFinalizeGrace = 60 * time.Second

func kitExecTimeout(wallClockSeconds int) time.Duration {
	return time.Duration(wallClockSeconds)*time.Second + kitFinalizeGrace
}
