package main

import (
	"time"
	"fmt"

	log "github.com/Sirupsen/logrus"

	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
	"io/ioutil"	
	"strconv"
	
	mgo "gopkg.in/mgo.v2"
	_ "gopkg.in/mgo.v2/bson"

)

// 常量
const (
	Environment = "production"
)

//配置文件
type conf struct {
	Dbconnection string `yaml:"dbconnection"`
	Dbconnectionthird string `yaml:"dbconnectionthird"`
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
	
		yamlFile, err := ioutil.ReadFile(absdir+"/config.yaml")
		if err != nil {
			log.Fatal(fmt.Sprintf("yamlFile.Get err   #%v ", err) )
		}
		err = yaml.Unmarshal(yamlFile, c)
		if err != nil {
			log.Fatal(fmt.Sprintf("Unmarshal: %v", err) )
		}
	
		log.Info("pid:",os.Getpid())
	
		pidPath := c.Pid
		pid :=  os.Getpid()
	
		err = ioutil.WriteFile(absdir+"/"+pidPath,[]byte(strconv.Itoa(pid)), 0644)
		if err != nil {
		  log.Info("error write pid to config file")//TODO
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


func main()  {	

	// Set log
	absdir := getCurrentDirectory();

	//加载配置文件里的配置
	var config conf
	config.getConf()

	log.Infof("config: %v", config)

	//日志文件设置
	logdir := absdir+"/log/zkcronad.log"
	log.Info("logdir:",logdir)

	f, err := os.OpenFile(logdir, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	defer f.Close()	


	defer func ()  {
		err := recover();
		if err != nil {
		  log.Fatalln(err)
		}
	}()


	//根据环境加载设定log等级
	if Environment == "production" {
		// Log as JSON instead of the default ASCII formatter.
		log.SetFormatter(&log.JSONFormatter{})
		// Output to stderr instead of stdout, could also be a file.
		log.SetOutput(f)
		// Only log the warning severity or above.
		log.SetLevel(log.WarnLevel)
	  } else {
		// The TextFormatter is default, you don't actually have to do this.
		log.SetFormatter(&log.TextFormatter{})
	
		log.SetLevel(log.InfoLevel)
	  }
	  
	// 初始化redis 
	cache := Cache{
		MaxIdle: 100,
		MaxActive: 100,
		IdleTimeoutSecs: 60,
		Address: config.Rediscacheaddress,
	}
	cache.Pool = cache.NewCachePool()

	cache2 := Cache{
		MaxIdle: 100,
		MaxActive: 100,
		IdleTimeoutSecs: 60,
		Address: config.Rediscacheaddress2,
	}
	cache2.Pool = cache2.NewCachePool()

	log.Infoln("== Redis Connect Ok ....")


	// 初始化mongodb
	host := []string{
		config.Dbconnection,
	}
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: host,
		Direct: true,
		Timeout: 5 * time.Second,
	})
	session.SetMode(mgo.Monotonic, true)
	if err != nil {
		log.Infoln("cannot connect to :" + config.Dbconnection)
		panic(err)
	}
	defer session.Close()

	log.Infoln("== Mongodb Connect Ok ....")
	

	// 初始化AppService
	appService := NewAppService(Environment ,cache, cache2, session, config.Workernum, config.Mysqlconnectaddr, config.Dbconnectionthird)
	log.Infof("appService: %v", appService)


	// 这两个方法需要预先加载一些缓存，所以启动的时候要运行

	// 加载广告分类category
	appService.PreloadCategoryCache()	
	// 加载第三方广告到缓存
	appService.PreloadThirdPartyAdCache()

	// 刷新广告缓存,这里可以不运行，startservice里面启动的定时任务会运行
	appService.PreloadAdsCache()

	//启动worker，加载cronjob
	appService.StartService()
		
	log.Info("== main func end ...")

}