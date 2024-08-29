// $ go test -run TestParser -v -count=1 pkg/worker/*.go

package worker

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParser(t *testing.T) {
	j := calculateNextPeriodic(&Job{
		Queue: "testParsing",
		Cron:  "0 2 * * *",
		Type:  TypePeriodic,
	})
	fmt.Println(j.RunAt)
}

func TestWorker(t *testing.T) {
	i := 0
	var wg sync.WaitGroup

	testHandler := func(args Args) error {
		i++
		bar := fmt.Sprintf("\r[%%-%vs]", i)
		fmt.Printf(bar, strings.Repeat("=", i)+">")
		wg.Done()
		return nil
	}

	d := NewDispatcher("worker_test", "redis://localhost:6379", 100)
	d.AddHandler("testQueued", testHandler)
	d.AddHandler("testPeriodic", testHandler)
	d.AddHandler("testScheduled", testHandler)

	// queued
	for i := 0; i < 50; i++ {
		d.EnqueueJob(&Job{
			Queue: "testQueued",
			Type:  TypeQueued,
		})
		wg.Add(1)
	}

	// periodic
	d.EnqueueJob(&Job{
		Queue: "testPeriodic",
		Cron:  "@every 1s",
		Type:  TypePeriodic,
	})
	wg.Add(2) // let it trigger 2 times

	// scheduled
	runAt := time.Now().Add(time.Second * 2)
	d.EnqueueJob(&Job{
		Queue: "testScheduled",
		RunAt: &runAt,
		Type:  TypeScheduled,
	})
	wg.Add(1)

	go d.Run()
	wg.Wait()
	d.Close()

	bar := fmt.Sprintf("\r[%%-%vs]", i)
	fmt.Printf(bar+" Done!\n", strings.Repeat("=", i))
}
