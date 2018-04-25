package main

import (
	"fmt"
	"github.com/jasonlvhit/gocron"
	"time"
)

func main() {
    for i := 0; i < 3; i++ {
        taskCron(i)
    }
    channel2 := make(chan int)
    go startCron(channel2)

    time.Sleep(time.Second * 6)
    gocron.Clear()
    fmt.Println("stop this")
}

func task(i int) {
	// time.Sleep(1* time.Second)
    fmt.Println("still running...", i)
}

func taskCron(i int) {
    gocron.Every(4).Seconds().Do(task, i)
}

func startCron(channel chan int) {
    <-gocron.Start()
}
