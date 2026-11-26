package publisher

import (
	"strings"
	"sync"
)

type WindowCounter struct {
	// key: repository file path; value: count of extracted snippet windows
	counts map[string]int
}

func NewWindowCounter() *WindowCounter {
	return &WindowCounter{counts: make(map[string]int)}
}

func (c *WindowCounter) RecordAll(paths []string) {
	var wg sync.WaitGroup
	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			c.counts[p]++
		}(path)
	}
	wg.Wait()
}

func ExtractLineWindow(content string, line, radius int) string {
	lines := strings.Split(content, "\n")
	start := line - radius
	if start < 0 {
		start = 0
	}
	end := line + radius
	return strings.Join(lines[start:end+1], "\n")
}
