package lib

import (
	log "github.com/Sirupsen/logrus"
	
	mgo "gopkg.in/mgo.v2"
	_ "gopkg.in/mgo.v2/bson"

	"github.com/jasonlvhit/gocron"
	"os"
	"time"
	"os/signal"
	"syscall"
	

)

type AppService struct {
	mode string
	cache Cache //cache
	adsCache Cache //ads_cache
	session *mgo.Session
}

func NewAppService(mode string, cache Cache, adsCache Cache,session *mgo.Session) AppService {
	return AppService{mode: mode, cache: cache, adsCache: adsCache, session: session}
}

// load test data 
func (app *AppService) MigrateData()  {
	log.Println("== AppService:MigrateData... ")

	values := "599405ffb09efea34c00000a"
	err := syncByCampaignId(app.cache, app.session, values, 1)
	log.Println(err)

}

// load all the ads cache 
func (app *AppService) PreloadAdsCache()  {
	log.Println("== AppService:preloadAdsCache... ")

	getDataFromMongo(app);

}

func (app *AppService) StartService()  {
	log.Println("== AppService:StartService... ")

	s := gocron.NewScheduler()
	s.Every(1).Seconds().Do(task1)
	s.Every(3).Seconds().Do(task2)

	sc := s.Start()
	// go test(s, sc)


	cancelChan := make(chan struct{})

	numberOfWorkers := 2

	// Worker running
	go SyncAdJob(cancelChan, numberOfWorkers, app.cache, app.session, ZK_ADS_SYNC_CAMPAIGN_QUEUE)


	// 接受终端停止信号
	stop := false
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		stop = true

		// 清理所有任务
		log.Println("Stopping...")
		s.Remove(task1)
		log.Println("removed task1...")
		s.Remove(task2)
		log.Println("removed task2...")
		
		log.Println("close cancel channel ... ")
		close(cancelChan)

		log.Println("waiting for 8 seconds ...")
		time.Sleep(8 * time.Second)

		log.Println("gocron clear...")
		s.Clear()
		
		log.Println("All task removed.....")
		close(sc) // close the channel

	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)


	<- sc

}


func task1(){
	log.Println("I am running task1....")
}

func task2(){
	log.Println("I am running task2.....")
}