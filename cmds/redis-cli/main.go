package main

import (
	"fmt"
    "time"
    "log"
    "os"
    "syscall"
    "os/signal"
    "github.com/eggegg/zkcronad/lib"

    "gopkg.in/yaml.v2"
	"io/ioutil"	
    "path/filepath"

    "math/rand"
)

// 常量
const (
	Environment = "development"
)

//配置文件
type conf struct {
	Dbconnection string `yaml:"dbconnection"`
	Rediscacheaddress string `yaml:"rediscacheaddress"`
	Rediscacheaddress2 string `yaml:"rediscacheaddress2"`
	Mysqlconnectaddr string `yaml:"mysqlconnectaddr"`
	Workernum int `yaml:"workernum"`
	Pid string `yaml:"pid"`
}

//读取配置文件，并且写入pid至pid文件
func (c *conf) getConf() *conf {
	
		absdir := getCurrentDirectory();
		log.Println(absdir)
	
		yamlFile, err := ioutil.ReadFile(absdir+"/../../config.yaml")
		if err != nil {
			log.Fatal(fmt.Sprintf("yamlFile.Get err   #%v ", err) )
		}
		err = yaml.Unmarshal(yamlFile, c)
		if err != nil {
			log.Fatal(fmt.Sprintf("Unmarshal: %v", err) )
		}
	
		return c
}
//获取当前目录
func getCurrentDirectory() string {
    if Environment == "development" {
      return "."
    }
    dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatal(err)
    }
    return dir

}

func main() {
    fmt.Println("stop this")

    // load config file and extract configuration
	var config conf
	config.getConf()

    log.Printf("config: %v", config)
    
    // init redis 
	cache := lib.Cache{
		MaxIdle: 100,
		MaxActive: 100,
		IdleTimeoutSecs: 60,
		Address: config.Rediscacheaddress,
	}
	cache.Pool = cache.NewCachePool()

    conn := cache.Pool.Get()
    defer conn.Close() // 关闭cache的redis连接
    
    // 接受终端停止信号
    signalChan := make(chan os.Signal, 1)
    done := make(chan struct{})
	go func() {
//ads_cache_single_ads_def_5993eeafb09efe1c4c000002 5a2faf66b09efe9944000006
        for {
            uuid := []string{
                // "5993e46db09efe354c000000",
                // "5993eb68b09efe364c000001",
                // "598315ceb09efe020800001a",
                "5993eeafb09efe1c4c000002",
                // "5994059cb09efe5c4c000005",
                // "55b5fe84bf0988ce7d00003d",
            }

            rand.Seed(time.Now().Unix()) // initialize global pseudo random generator
            randId := uuid[rand.Intn(len(uuid))]
            log.Println("== id: ", randId)

            if _, err := conn.Do("RPUSH", lib.ZK_ADS_SYNC_CAMPAIGN_QUEUE, randId); err != nil {
                log.Fatal("cannot enqueue value")
                break
            } else {
                log.Println("enqueue success..")
            }
            
            time.Sleep(5 * time.Second)            


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
