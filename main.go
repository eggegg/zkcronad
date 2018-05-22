package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/urfave/cli"
	"os"
	"path/filepath"

	
	mgo "gopkg.in/mgo.v2"
	_ "gopkg.in/mgo.v2/bson"

	"github.com/eggegg/zkcronad/configuration"
	"github.com/eggegg/zkcronad/lib"
)

// 常量
const (
	Environment = "development"
)

func getCurrentDirectory() string {
    if Environment == "development" {
      return "."
    }
    dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatal(err)
    }
    // return strings.Replace(dir, "\\", "/", -1)
    return dir

}

func main()  {
	
	defer func ()  {
		err := recover();
		if err != nil {
		  log.Error(err)
		}
	}()

	// Set log
	absdir := getCurrentDirectory();
	logdir := absdir+"/log/zkcronad.log"
	log.Info("logdir:",logdir)

	f, err := os.OpenFile(logdir, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	// don't forget to close it
	defer f.Close()	

	//根据环境加载设定log等级
	if Environment == "production" {
		// Log as JSON instead of the default ASCII formatter.
		log.SetFormatter(&log.JSONFormatter{})
		// Output to stderr instead of stdout, could also be a file.
		log.SetOutput(f)
		// Only log the warning severity or above.
		log.SetLevel(log.ErrorLevel)
	  } else {
		// The TextFormatter is default, you don't actually have to do this.
		log.SetFormatter(&log.TextFormatter{})
	
		log.SetLevel(log.DebugLevel)
	  }

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

	cache2 := lib.Cache{
		MaxIdle: 100,
		MaxActive: 100,
		IdleTimeoutSecs: 60,
		Address: config.RedisCacheAddress2,
	}
	cache2.Pool = cache2.NewCachePool()

	// init mongodb
	session, err := mgo.Dial(config.DBConnection)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	// Init AppService
	appService := lib.NewAppService(Environment ,cache, cache2, session)

	log.Printf("appService: %v", appService)

	

	// cli app set
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "category",
			Value: "no",
			Usage: "refresh the category (yes/no)",
		},
		cli.StringFlag{
			Name: "refresh",
			Value: "yes",
			Usage:"Refresh all the cache now (yes/no)",
		},
		cli.StringFlag{
			Name: "start",
			Value: "no",
			Usage: "Start cron job (yes/no)",
		},
	}

	app.Version = "1.0"
	// define action
	app.Action = func (c *cli.Context) error {
		log.Println(c.String("refresh"), c.String("start"))

		if c.String("category") == "no" {
			log.Println("Skip migrate data")
		} else {
			appService.PreloadCategoryCache(config.MysqlConnectAddr)
			os.Exit(0)			
		}

		// check the flag value
		if c.String("refresh") == "no" {
			log.Println("Skip refresh all cache")
		} else {
			appService.PreloadAdsCache()
		}

		if c.String("start") == "no" {
			log.Println("Skip start cron job")
		} else {
			appService.PreloadCategoryCache(config.MysqlConnectAddr)
			appService.StartService()
		}

		return nil
	}

	cliErr := app.Run(os.Args)
	if cliErr != nil {
		log.Fatal(cliErr)
	}

}