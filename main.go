package main

import (
	"fmt"
	"github.com/jasonlvhit/gocron"
	"github.com/urfave/cli"
	"time"
	"log"
	"os"
)

func main()  {
	
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "refresh",
			Value: "no",
			Usage:"Refresh all the cache now (yes/no)",
		},
		cli.StringFlag{
			Name: "start",
			Value: "no",
			Usage: "Start cron job",
		},
	}

	app.Version = "1.0"
	// define action
	app.Action = func (c *cli.Context) error {
		log.Println(c.String("refresh"), c.String("start"))

		// check the flag value
		if c.String("refresh") == "no" {
			log.Println("Skip refresh all cache")
		} else {
			refreshAll()
		}

		if c.String("start") == "no" {
			log.Println("Skip start cron job")
		} else {
			startService()
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}


func task(){
	fmt.Println("I am running task")
}

func taskWithParams(a int, b string)  {
	fmt.Println(a, b)
}
func refreshAll()  {
	log.Println("Refresh all the cache")
}

func startService()  {
	
	s := gocron.NewScheduler()
	s.Every(1).Seconds().Do(task)
	s.Every(4).Seconds().Do(taskWithParams, 1, "hello world")

	sc := s.Start()
	go test(s, sc)
	<- sc
}

func test(s *gocron.Scheduler, sc chan bool)  {
	time.Sleep(8 * time.Second)
    s.Remove(task) //remove task
    time.Sleep(8 * time.Second)
    s.Clear()
    fmt.Println("All task removed")
    close(sc) // close the channel
}