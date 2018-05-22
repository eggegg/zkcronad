package main

import (
	"fmt"
    "time"
    "log"
    "os"
    "syscall"
    "os/signal"
    "github.com/eggegg/zkcronad/lib"
    "github.com/eggegg/zkcronad/configuration"
)

func main() {
    fmt.Println("stop this")

    // load config file and extract configuration
	confPath := "./configuration/config.json"
	config, _ := configuration.ExtractConfiguration(confPath)

    log.Printf("config: %v", config)
    
    // init redis 
	cache := lib.Cache{
		MaxIdle: 100,
		MaxActive: 100,
		IdleTimeoutSecs: 60,
		Address: config.RedisCacheAddress,
	}
	cache.Pool = cache.NewCachePool()

    conn := cache.Pool.Get()
    defer conn.Close() // 关闭cache的redis连接
    
    // 接受终端停止信号
    signalChan := make(chan os.Signal, 1)
    done := make(chan struct{})
	go func() {

        for {
            uuid :=  "5993eb68b09efe364c000001"

            if _, err := conn.Do("RPUSH", lib.ZK_ADS_SYNC_CAMPAIGN_QUEUE, uuid); err != nil {
                log.Fatal("cannot enqueue value")
                break
            } else {
                log.Println("enqueue success..")
            }
            
            time.Sleep(1 * time.Second)            

            // Listen for the cancel channel
            select {
                case <- signalChan:
                    log.Println("All task removed.....")                    
                    close(done)                    
                    return
                default:
            }
        }
		

	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

    <-done
}
