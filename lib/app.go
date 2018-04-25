package lib

import (
	log "github.com/Sirupsen/logrus"
	
	mgo "gopkg.in/mgo.v2"
	_ "gopkg.in/mgo.v2/bson"

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

func (app *AppService) Task1()  {
	log.Println("== AppService:task1... ")
}