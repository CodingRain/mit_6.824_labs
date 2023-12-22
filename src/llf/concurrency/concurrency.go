package main

import (
	"math/rand"
	"sync"
	"time"
)

func main() {
	count := 0
	finish := 0
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finish++
		}()
	}

	for {
		mu.Lock()
		if count >= 5 || finish == 10 {
			break
		}
		mu.Unlock()
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}

}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Intn(100) > 50
}
