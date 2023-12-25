package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second * 1)
		cancel()
	}()
	sleep(ctx)
}

func sleep(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// time.Sleep(time.Second * 5)
			fmt.Println("haha")
		}
	}
}
