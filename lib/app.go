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
	"fmt"


)

type AppService struct {
	mode string
	cache Cache //cache
	adsCache Cache //ads_cache
	session *mgo.Session
	workernum int
}

func NewAppService(mode string, cache Cache, adsCache Cache,session *mgo.Session, workernum int) AppService {
	return AppService{mode: mode, cache: cache, adsCache: adsCache, session: session, workernum: workernum }
}

// 加载广告category 
func (app *AppService) PreloadCategoryCache(mysqlconnectaddr string) {

	log.Info("== AppService: PreloadCategoryCache, mysqlconnectaddr ,", mysqlconnectaddr)

	fromTimestamp := time.Now()
	loadCategoryFromDbToCache(app, mysqlconnectaddr)	
	log.Warnf("== AppService:PreloadCategoryCache finish, Using %v seconds", time.Since(fromTimestamp).Seconds())
}

func (app *AppService) PreloadThirdPartyAdCache(dbconnection string)  {
	log.Info("== AppService: PreloadThirdPartyAdCache, mongoaddress:", dbconnection)

	loadThirdPartyFromDbToCache(app, dbconnection)
}

// 刷新广告缓存
func (app *AppService) PreloadAdsCache()  {
	log.Info("== AppService:preloadAdsCache begin ... ")

	fromTimestamp := time.Now()
	getDataFromMongo(app);
	
	log.Warnf("== AppService:preloadAdsCache finish, Using %v seconds", time.Since(fromTimestamp).Seconds())

}

func (app *AppService) task1(){
	log.Println("== AppService:tast1 running ...")
}

func (app *AppService) task2(){
	log.Println("== AppService:tast2 running ...")
}

// 开启定时任务
func (app *AppService) StartService()  {

	log.Println("== AppService:StartService... ")

	s := gocron.NewScheduler()

	//设置定时任务
	// s.Every(5).Seconds().Do(app.task1)
	s.Every(300).Seconds().Do(app.PreloadAdsCache)

	sc := s.Start()

	// channel for cancel
	cancelChan := make(chan struct{})

	// 开启处理广告更新队列的worker，数量为配置的workernum
	numberOfWorkers := app.workernum
	// 同步广告计划的worker
	go SyncAdJob(cancelChan, numberOfWorkers, app.cache, app.session, ZK_ADS_SYNC_CAMPAIGN_QUEUE)
	// 同步广告主的worker
	go SyncAdJob(cancelChan, 1, app.cache, app.session, ZK_ADS_SYNC_ADVERTISER_QUEUE)
	
	// 接受终端停止信号
	stop := false
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		stop = true

		// 清理所有任务
		fmt.Println("Stopping...")
		s.Remove(app.task1)
		log.Println("removed task1...")
		s.Remove(app.PreloadAdsCache)
		log.Println("removed preloadAdsCache...")
		
		//发送cancel信号，停用worker
		log.Println("close cancel channel ... ")
		close(cancelChan)

		//等待8秒，让正在执行的任务执行完毕
		fmt.Println("waiting for 8 seconds ...")
		time.Sleep(8 * time.Second)

		//清理计划任务并退出程序
		fmt.Println("gocron clear...")
		s.Clear()
		
		fmt.Println("All task removed.....")
		close(sc) // close the channel

	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<- sc

}
