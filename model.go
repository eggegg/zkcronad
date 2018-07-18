package main

import (
	"time"
	log "github.com/Sirupsen/logrus"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"encoding/json"

	"strings"

	redigo "github.com/garyburd/redigo/redis"

	"errors"
	"fmt"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	
)


func getDataFromMongo(app *AppService)  {
	log.Infof("== Beginning func getDataFromMongo....")

	session := app.session.Copy()
	defer session.Close()

	// Redis
	conn := app.cache.Pool.Get()
	defer conn.Close() // 关闭cache的redis连接

	// 广告缓存找出所有category
	var allAdsCategoryMap map[string]string
	allAdsCategoryMap, hmErr := redigo.StringMap(conn.Do("HGETALL", ZK_ADS_CACHE_ALL_CATEGORY));
	if hmErr != nil{
		log.Println(hmErr)
	} 
	log.Infof("== category map len: %v", len(allAdsCategoryMap))

	//第三方广告白名单
	adThirdParty := map[string][]string{}
	adThirdParty, _ = getAdThirdPartyMapFromCache(app.cache)
	
	// Begin Redis Transaction
	// conn.Send("MULTI")

	// Get Campaign Space
	var campaignSpaces []CampaignSpace
	
	nowTimeStamp := time.Now().Unix()
	if app.mode == "development" {
		nowTimeStamp = 1523849239
	}
	log.Infof("== search for campaign nowTimestamp: %v", nowTimeStamp)

	// START 删除状态不正常的广告缓存
	// var  unNormalAid []struct {
	// 	Id bson.ObjectId `bson:"_id"`
	// }

	c := session.DB("zk_dsp").C("campaign_space")
	log.Infof("== get session: %v", c)

	// unNormalErr := c.Find(bson.M{
	// 	"start_time": bson.M{"$lt": nowTimeStamp},
	// 	"end_time": bson.M{"$gt": nowTimeStamp},
	// 	"stat" : bson.M{"$ne" : 1},
	// }).Select(bson.M{"_id":1}).All(&unNormalAid)
	// if unNormalErr != nil {
	// 	log.Errorf("== err searching unnormal ad: %v ", unNormalErr)
	// }
	// log.Infof("== find unnormalad num : %v", len(unNormalAid) )
	
	// if len(unNormalAid) > 0 {
	// 	for _, v := range unNormalAid {
	// 		conn.Send("DEL", strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, v.Id.Hex()}, ""))			
	// 	}
	// }
	// END 删除状态不正常的广告缓存 

	// 找出所有可用广告
	beginTimeStamp := time.Now()
	err := c.Find(bson.M{
		"start_time": bson.M{"$lt": nowTimeStamp},
		"end_time": bson.M{"$gt": nowTimeStamp},
		"stat" : 1,
		"no_balance" : bson.M{"$ne" : 1},
	}).All(&campaignSpaces)

	if err != nil {
		log.Error(err)
	}
	log.Warnf("Mongodb number of campaign_space: (%d), using : %v", len(campaignSpaces), time.Since(beginTimeStamp));
	
	//广告所属的广告计划ID
	var allCampaignIds []string
	for _, v := range campaignSpaces {
		allCampaignIds = append(allCampaignIds, v.Ad_group_id)
	} 	
	campaignIds := getUniqueString(allCampaignIds)
	log.Warnf("Number of unique campaign of all the campaign_space: (%d) ", len(campaignIds))

	var campaignBsonIds []bson.ObjectId
	for _, id := range campaignIds {
		campaignBsonIds = append(campaignBsonIds, bson.ObjectIdHex(id))
	}
	
	//找出所有广告计划
	beginTimeStamp = time.Now()
	c = session.DB("zk_dsp").C("campaign")
	var campaigns []Campaign

	err = c.Find(bson.M{
		"status" : 2,
		"_id": bson.M{"$in": campaignBsonIds},
	}).All(&campaigns)
	if err != nil {
		log.Error(err)
	}

	campaignsMap :=  make(map[string]Campaign, len(campaigns))
	
	for _, v := range campaigns {
		campaignsMap[v.Id.Hex()] = v
	}

	log.Warnf("Load campaign num: (%v) , using : %v", len(campaigns), time.Since(beginTimeStamp))


	// 找出所有创意
	beginTimeStamp = time.Now()
	c = session.DB("zk_dsp").C("creative")
	var creatives []Creative
	var creativesIds []string

	err = c.Find(bson.M{
		"status" : 2,
		"campaignid" : bson.M{"$in": campaignIds},
	}).All(&creatives)
	if err != nil {
		log.Error(err)
	}


	creativesMap := map[string]map[string]Creative{}
	for _, v := range creatives {
		creativesIds = append(creativesIds, v.Id.Hex())
		campaignCreatives := creativesMap[v.Campaignid]
		if campaignCreatives == nil {
			campaignCreatives = make(map[string]Creative)
			creativesMap[v.Campaignid] = campaignCreatives
		}
		campaignCreatives[v.Id.Hex()] = v
	}

	log.Warnf("Load creatives num: (%v) , using: (%v)", len(creatives), time.Since(beginTimeStamp) )


	/*
		每条广告设置缓存
		全部广告id设置缓存
		广告id按属性设置缓存
	*/

	var oneAdsId, oneAdsCampaignId string
	var allAdsId []string
	adsGroup := map[string][]string{}
	adsGroupMap := map[string]bool{}
	
	for _, oneAds := range campaignSpaces {
		// log.Printf("== %v, %T", oneAds, oneAds.Id.Hex())
		oneAdsId = oneAds.Id.Hex()
		oneAdsCampaignId = oneAds.Ad_group_id
		allAdsId = append(allAdsId, oneAdsId)

		//删除状态不正常的广告缓存
		if oneAds.Stat != 1 {
			conn.Send("DEL", strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, ""))
			continue
		}

		oneAdsCampaign, ok := campaignsMap[oneAdsCampaignId]
		if ok != true {
			log.Error("error get campaign info of id:", oneAdsCampaignId)
		}

		//删除状态不正常的广告
		if oneAds.Stat != 1 {
			conn.Send("DEL", strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, ""))	
			continue
		}

		//加入广告组定义
		oneAds.Ad_group_def.Id = oneAdsCampaignId
		oneAds.Ad_group_def.Target_clicks = oneAdsCampaign.Target_clicks
		oneAds.Ad_group_def.Daily_target_clicks = oneAdsCampaign.Daily_target_clicks
		oneAds.Ad_group_def.Target_views = oneAdsCampaign.Target_views
		oneAds.Ad_group_def.Daily_target_views = oneAdsCampaign.Daily_target_views

		if oneAdsCampaign.Target_clicks > 0 {
			oneAds.Target_clicks = oneAdsCampaign.Target_clicks
		}
		if oneAdsCampaign.Daily_target_clicks > 0 {
			oneAds.Daily_target_clicks = oneAdsCampaign.Daily_target_clicks
		}

		oneAds.Target_views = oneAdsCampaign.Target_views
		oneAds.Daily_target_views = oneAdsCampaign.Daily_target_views

		//给有勾选喜好分类的广告加上“新闻”分类
		if len(oneAds.Favour_category) > 0 && !stringInSlice("15", oneAds.Favour_category) {
			oneAds.Favour_category = append(oneAds.Favour_category, "15")
		}

		//创意
		oneCreatives, ok := creativesMap[oneAdsCampaignId]
		if ok == true {
			// log.Printf("== find creative num: %v", len(oneCreatives))
			oneAds.Creatives = oneCreatives
		} else {
			//没有广告创意的不加载
			continue
		}

		//将广告所属小类加入广告标签
		if len(oneAds.Category) > 0  {
			oneAdCategory, ok := allAdsCategoryMap[oneAds.Category]
			if ok == true {
				oneAds.Tags = append(oneAds.Tags, oneAdCategory)
			}
		}

		//广告白名单
		if oneForThirdParty, ok := adThirdParty[oneAdsCampaignId]; ok {
			oneAds.For_third_party = oneForThirdParty
		}

		//设置单条缓存和过期时间
		oneAdsJson, err := json.Marshal(oneAds)	
		if err != nil {
			log.Error("== failed encode json of campaign_id:", oneAdsId)
		}			
		oneAdsRedisKey := strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, "")
		conn.Send("SET", oneAdsRedisKey, oneAdsJson)
		conn.Send("EXPIRE", oneAdsRedisKey, ZK_ADS_CACHE_SINGLE_SET_KEY)

		//按条件设置缓存
		setKeys := getPreloadRedisKeys(&oneAds)
		// log.Printf(">>> get setkeys of :%v, len(%v)", oneAdsId, len(setKeys))

		for _, value := range setKeys {
			_, ok := adsGroupMap[value] 
			if !ok {
				adsGroupMap[value] = true
				adsGroup[value] = []string{oneAdsId}
			} else {
				adsGroup[value] = append(adsGroup[value], oneAdsId)
			}
		}

	}

	// for key, value := range adsGroup {
	// 	log.Warnf("%v : %v", key, value)
	// }

	log.Infof("=== finish adsGroup : %v, %v", len(adsGroup), len(adsGroupMap))

	//先删除再设置全部广告ID缓存
	conn.Send("DEL", ZK_ADS_CACHE_ALL_ADS_SET)
	for _, id := range allAdsId {
		conn.Send("SADD", ZK_ADS_CACHE_ALL_ADS_SET, id)
	}
	conn.Send("EXPIRE", ZK_ADS_CACHE_ALL_ADS_SET, ZK_ADS_CACHE_SINGLE_SET_KEY)
	
	log.Warnf("Redis load campaign_space num (%v) ", len(allAdsId))

	//处理广告属性的redis集合设置
	for key, _ := range adsGroupMap {
		conn.Send("DEL", key)
	}
	for key, value := range adsGroup {
		for _, id := range value {
			conn.Send("SADD", key, id)
		}
		conn.Send("EXPIRE", key, ZK_ADS_CACHE_SINGLE_SET_KEY)
	}
	log.Warnf("Redis load cache key adsGroup : %v, %v", len(adsGroup), len(adsGroupMap))
	
	_, err = conn.Do("")
	if err != nil {
		log.Errorln("Pipeline ads err:", err)
	}

	// _, err = conn.Do("EXEC")
	// if err != nil {
	// 	log.Error("=295= redis multi exec error: ", err)
	// }
	log.Info("== redis multi exec command finished" )



	/*
		更新广告的曝光数和点击数缓存
		更新广告计划的曝光数和点击数缓存
		更新创意的曝光数和点击数缓存
		重建广告主缓存
	*/

	// 获取ad_cache的redis连接
	adsConn := app.adsCache.Pool.Get()

	defer adsConn.Close() 
	

	// Begin Redis Transaction
	// adsConn.Send("MULTI")

	//更新广告的曝光数和点击数
	c = session.DB("zk_ads_stat").C("ads_action_stat")
	
	var adsStats []AdsStat
	err = c.Find(bson.M{
		"ads_id" : bson.M{"$in": allAdsId},
	}).All(&adsStats)
	if err != nil {
		log.Error(err)
	}
	log.Warnf("Get ads stat num (%v)", len(adsStats))

	if len(adsStats) > 0 {
		for _, stat := range adsStats {
			if stat.Ads_id == "" {
				continue
			}
			if stat.Show_count > 0 {
				log.Debugf("set ad_id:%s show:%v", stat.Ads_id, stat.Show_count)									
				showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id},""), "as_", 16)
				adsConn.Send("SETEX", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Show_count)
			}
			if stat.Click_count > 0 {
				log.Debugf("set ad_id:%s click:%v", stat.Ads_id, stat.Click_count)													
				clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id},""), "ac_", 16)
				adsConn.Send("SETEX", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Click_count)
			}
		}
	}

	//更新广告计划的曝光数和点击数
	var adsCampaignStats []AdsStat
	err = c.Find(bson.M{
		"ads_id" : bson.M{"$in": allCampaignIds},
	}).All(&adsCampaignStats)
	if err != nil {
		log.Error(err)
	}
	log.Debugf("== get campaign stat: %v", adsCampaignStats)

	todayDateFormat := time.Now().Format("2006-01-02")

	if len(adsCampaignStats) > 0 {
		for _, stat := range adsCampaignStats {
			if stat.Ads_id == "" {
				continue
			}
			if stat.Show_count > 0 {
				log.Debugf("set campaign_id:%s show:%v", stat.Ads_id, stat.Show_count)
				showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id},""), "as_", 16)
				adsConn.Send("SETEX", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Show_count)
			}
			if stat.Click_count > 0 {
				log.Debugf("set campaign_id:%s click:%v", stat.Ads_id, stat.Show_count)				
				clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id},""), "ac_", 16)
				adsConn.Send("SETEX", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Click_count)
			}

			if len(stat.Daily_shows) > 0 {
				todayShow, ok := stat.Daily_shows[todayDateFormat]
				if ok == true && todayShow > 0 {
					log.Debugf("set campaign_id:%s daily_show:%v", stat.Ads_id, stat.Show_count)					
					showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id,"_",todayDateFormat}, ""), "as_", 16)
					adsConn.Send("SETEX", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, todayShow)
				}
			}
			if len(stat.Daily_clicks) > 0 {
				todayClick, ok := stat.Daily_clicks[todayDateFormat]
				if ok == true && todayClick > 0 {
					log.Debugf("set campaign_id:%s daily_click:%v", stat.Ads_id, stat.Show_count)										
					clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id,"_",todayDateFormat}, ""), "ac_", 16)				
					adsConn.Send("SETEX", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE, todayClick)
				}
			}
		}
	}

	//更新创意的曝光和点击数
	c = session.DB("zk_ads_stat").C("ads_creative_stat")

	var creativeStats []CreativeStat
	err = c.Find(bson.M{
		"creativeid" : bson.M{"$in": creativesIds},
	}).All(&creativeStats)
	if err != nil {
		log.Error(err)
	}
	log.Warnf("Load creatives stat (%v)", len(creativeStats))

	if len(creativeStats) > 0 {

		var today, yesterday, dayBeforeYesterday string

		if app.mode == "development" {
			//测试模式使用测试数据
			name, offset := time.Now().Zone()
			log.Println("== find timezone:", name, offset)
			currenttime := time.Unix(nowTimeStamp+int64(offset), 0)
			today = currenttime.Format("2006-01-02")
			yesterday = currenttime.AddDate(0, 0, -1).Format("2006-01-02")
			dayBeforeYesterday = currenttime.AddDate(0,0,-2).Format("2006-01-02")
		} else {
			today = time.Now().Format("2006-01-02")		
			yesterday = time.Now().AddDate(0, 0, -1).Format("2006-01-02")
			dayBeforeYesterday = time.Now().AddDate(0,0,-2).Format("2006-01-02")
		}

		
		log.Printf("== today: %v, yesterday: %v, dayBeforeYesterday:%v", today, yesterday, dayBeforeYesterday)
		dayKeys := []string{today, yesterday, dayBeforeYesterday}

		for _, stat := range creativeStats {
			creativeShowKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_SHOW_COUNT, stat.Creative_id}, ""), "cs_", 16)
			creativeClickKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_CLICK_COUNT, stat.Creative_id}, ""), "cc_", 16)

			adsConn.Send("SETEX", creativeShowKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Show_count)
			adsConn.Send("SETEX", creativeClickKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Click_count)

			
			if len(stat.Daily_shows) > 0 {
				//获取最近三日的曝光和统计
				for _, day := range dayKeys {
					count, ok := stat.Daily_shows[day]; 
					if ok==true && count > 0 {
						// log.Printf("== found show for key %v, value: %v", day, count)
						dailyShowKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_SHOW_COUNT, stat.Creative_id, "_", day}, ""), "cs_", 16)
						adsConn.Send("SETEX", dailyShowKey, ZK_ADS_ADS_CACHE_EXPIRE, count)
					}
				} 
			}
			if len(stat.Daily_clicks) > 0 {
				for _, day := range dayKeys {
					count, ok := stat.Daily_clicks[day]; 
					if ok == true && count > 0{
						// log.Printf("== found click for key %v, value: %v", day, count)
						dailyClickKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_CLICK_COUNT, stat.Creative_id, "_", day}, ""), "cc_", 16)
						adsConn.Send("SETEX", dailyClickKey, ZK_ADS_ADS_CACHE_EXPIRE, count)
					}
				}
			}	
		}
	}

	//重建广告主缓存
	var advertisers []Advertiser
	c = session.DB("zk_dsp").C("advertiser")
	err = c.Find(bson.M{
		"type" : bson.M{"$ne": 1},
	}).All(&advertisers)
	if err != nil {
		log.Error(err)
	}
	log.Infof("== get advertiser count: %v", len(advertisers))

	advCacheKey := strings.Replace(ZK_DSP_CACHE_ADVERTISER_DATA, "{id}", "", 1)
	
	for _, adv := range advertisers {
		oneAdvJson, err := json.Marshal(adv)	
		if err != nil {
			log.Error("== failed encode json of advtiser:", adv.Id.Hex())
		}		
		adsConn.Send("SETEX", strings.Join([]string{advCacheKey, adv.Id.Hex()}, ""), 86400, oneAdvJson)
	}
	
	log.Warnf("Load advertiser num (%d)", len(advertisers))
	
	// adsConn.Flush()
	// v, err := adsConn.Receive()
	// log.Warnln("pipeline result:", v)

	_, err = adsConn.Do("")
	if err != nil {
		log.Errorln("Pipeline ads_cache err:", err)
	}

	log.Warnln("Pipeline ads_cache ok!")

	// 批量执行
	// _, err = adsConn.Do("EXEC")
	// if err != nil {
	// 	log.Error("=490= redis multi exec error: ", err)
	// }


	log.Debugf("== redis multi exec command finished" )

	log.Printf("== all reload tasks finished successfully")

}



// 查找某一个广告所有的属性组合key
func getPreloadRedisKeys(ad *CampaignSpace) []string {
	setKeys := []string{}

	adGroup := ad.Ads_group

	if ad.Is_test == 1 {
		//如果是测试广告

		if len(ad.Device_type) > 0 {
			for _, deviceId := range ad.Device_type {
				setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_FAV_ADS_SET,deviceId,adGroup, "99"}, "") )
			}
		}

		if len(ad.Location) > 0 {
			if len(ad.Device_type) >0 {
				for _, deviceId := range ad.Device_type {
					for _,locationId := range ad.Location {
						setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_LOCATION_ADS_SET,deviceId,adGroup,locationId, "_test"}, ""))
					}
				}
			}
		
		}else {

		
			if len(ad.Device_type) >0 && len(ad.Channel) > 0 {
				for _, deviceId := range ad.Device_type {
					setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_CHANNEL_ADS_SET,deviceId,adGroup,"862"}, ""))
				}
			}
		}

	} else {

		//按平台，分组，喜好归总
		if len(ad.Location) > 0 {
	
			if len(ad.Device_type) >0 {
				for _, deviceId := range ad.Device_type {
					for _,locationId := range ad.Location {
						setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_LOCATION_ADS_SET,deviceId,adGroup,locationId}, ""))
					}
				}
			}
	
		} else {
	
			if len(ad.Device_type) > 0 && len(ad.Favour_category) > 0 {
				for _, deviceId := range ad.Device_type {
					for _, favId := range ad.Favour_category {
						setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_FAV_ADS_SET,deviceId,adGroup, favId}, "") )
					}
				}
			}
	
			if len(ad.Device_type) > 0 && len(ad.Tags) > 0 {
				for _, deviceId := range ad.Device_type {
					for _, tagId := range ad.Tags {
						setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_TAG_ADS_SET,deviceId,adGroup,tagId}, ""))
					}
				}
			}
	
			if len(ad.Device_type) >0 && len(ad.Channel) > 0 {
				for _, deviceId := range ad.Device_type {
					for _, channelId := range ad.Channel {
						setKeys = append(setKeys, strings.Join([]string{ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_CHANNEL_ADS_SET,deviceId,adGroup,channelId}, ""))
					}
				}
			}
	
		}

	}

	

	return setKeys
}


/*
	uuid=广告计划id
	广告计划修改之后，同步缓存
*/
func syncByCampaignId(cache Cache, session *mgo.Session, uuid string, workerId int)  error{

	log.Infof("== begin sync worker id: %v , syncByCampaignId: %v ....", workerId, uuid)

	// MongoDB session
	db := session.Copy()
	defer db.Close()

	// Redis
	conn := cache.Pool.Get()
	defer conn.Close()
	
	var campaignSpaces []CampaignSpace
	oldCampaignSpaceMap := make(map[string]CampaignSpace)
	var allCampaignSpacesId []string
	var campaign Campaign
	var creatives []Creative
	campaignCreatives := make(map[string]Creative)
	

	// Campaign
	c := db.DB("zk_dsp").C("campaign")
	err := c.FindId(bson.ObjectIdHex(uuid)).One(&campaign)
	if err != nil {
		return errors.New("cannot found campiagn ")
	}

	// Campaign space
	c = db.DB("zk_dsp").C("campaign_space")
	err = c.Find(bson.M{
		"ad_group_id" :  uuid,
	}).All(&campaignSpaces)
	if err != nil {
		return err
	}

	for _, value := range campaignSpaces {
		allCampaignSpacesId = append(allCampaignSpacesId, value.Id.Hex())
	}
	oldCampaignSpaceMap, _ = getCampaignSpaceByIds(cache, allCampaignSpacesId)
	log.Info("== get campaignspace from db num: %v , old campaignspace num: %v", len(campaignSpaces), len(oldCampaignSpaceMap) )
	
	// Creatives
	c = db.DB("zk_dsp").C("creative")
	err = c.Find(bson.M{
		"status" : 2,
		"campaignid" : uuid,
	}).All(&creatives)
	if err != nil {
		log.Error(err)
	}
	log.Infof("== found creatives num: %v", len(creatives))

	if len(creatives) > 0 {
		for _, v := range creatives {
			campaignCreatives[v.Id.Hex()] = v
		}	
	}
	
	//找出该广告的分类
	oneAdsCategory, _ := redigo.String( conn.Do("HGET", ZK_ADS_CACHE_ALL_CATEGORY, campaign.Category)	) 
		

	// Begin Redis Transaction
	// conn.Send("MULTI")

	// loop all the campaign spaces
	for _, oneAds := range campaignSpaces {

		oneAdsId := oneAds.Id.Hex()
		oneAdsCampaignId := oneAds.Ad_group_id


		//如果没有创意，或者广告不合法，删除广告位缓存
		nowTimeStamp := int(time.Now().Unix())
		if !( len(creatives) > 0 )  || oneAds.Start_time > nowTimeStamp || oneAds.End_time < nowTimeStamp || oneAds.Stat != 1 {
			// conn.Send("DEL", strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, ""))			
			continue
		}
		

		//加入广告组定义
		oneAds.Ad_group_def.Id = oneAdsCampaignId
		oneAds.Ad_group_def.Target_clicks = campaign.Target_clicks
		oneAds.Ad_group_def.Daily_target_clicks = campaign.Daily_target_clicks
		oneAds.Ad_group_def.Target_views = campaign.Target_views
		oneAds.Ad_group_def.Daily_target_views = campaign.Daily_target_views

		if campaign.Target_clicks > 0 {
			oneAds.Target_clicks = campaign.Target_clicks
		}
		if campaign.Daily_target_clicks > 0 {
			oneAds.Daily_target_clicks = campaign.Daily_target_clicks
		}

		oneAds.Target_views = campaign.Target_views
		oneAds.Daily_target_views = campaign.Daily_target_views

		//给有勾选喜好分类的广告加上“新闻”分类
		if len(oneAds.Favour_category) > 0 && !stringInSlice("15", oneAds.Favour_category) {
			oneAds.Favour_category = append(oneAds.Favour_category, "15")
		}

		//创意
		oneAds.Creatives = campaignCreatives

		//将广告所属小类加入广告标签
		if len(oneAdsCategory) > 0 {
			oneAds.Tags = append(oneAds.Tags, oneAdsCategory)
		}

		//广告的第三方渠道白名单
		oldCampaignSpace, oldExists := oldCampaignSpaceMap[oneAdsId]
		log.Infof("== find old: %v", oldExists)
		if oldExists != true {
			// 旧缓存不存在,再拿cache里的
			adThirdParty := map[string][]string{}
			adThirdParty, err := getAdThirdPartyMapFromCache(cache)
			if err != nil {
				log.Info(err)
			}

			if forThirdParty, ok := adThirdParty[oneAds.Ad_group_id];  ok {
				oneAds.For_third_party = forThirdParty
			}

		} else {
			// 有旧缓存就直接用旧缓存的
			if len(oldCampaignSpace.For_third_party) >0 {
				log.Info("== use old For_third_party ")				
				oneAds.For_third_party = oldCampaignSpace.For_third_party
			}
		}
		
		//设置单条缓存和过期时间
		oneAdsJson, err := json.Marshal(oneAds)	
		if err != nil {
			log.Error("== failed encode json of campaign_id:", oneAdsId)
			return err
		}			
		oneAdsRedisKey := strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, "")			

		conn.Send("SET", oneAdsRedisKey, oneAdsJson)
		conn.Send("EXPIRE", oneAdsRedisKey, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)
		log.Info("== set oneads ok ", oneAdsRedisKey)

		//对比旧的数据和新的数据，更新对应的属性
		if oldExists != true {
			// 没有找到旧campaign的时候，全部添加
			addKeys := getPreloadRedisKeys(&oneAds) 
			if len(addKeys) >0 {
				for _, key := range addKeys {
					conn.Send("SADD", key, oneAdsId)
					conn.Send("EXPIRE", key, ZK_ADS_CACHE_SINGLE_SET_KEY)
				}
			}

			log.Infof("== [insert] addKeys num :  %v",len(addKeys))
			

		} else {
			// 有旧数据，做对比
			oldSetKeys := getPreloadRedisKeys(&oldCampaignSpace)
			newSetKeys := getPreloadRedisKeys(&oneAds) 
			log.Infof("== [update] oldSetKeys num: %v, newSetKeys num: %v", len(oldSetKeys), len(newSetKeys))
		
	
			deleteKeys := differentOfSlicesString(oldSetKeys, newSetKeys)
			addKeys := differentOfSlicesString(newSetKeys, oldSetKeys)
			log.Infof("== [update] deleteKeys num: %v, addKeys num :  %v", len(deleteKeys), len(addKeys))
			if len(deleteKeys) > 0 {
				for _, key := range deleteKeys {
					conn.Send("SREM", key, oneAdsId)
				}
			}
			if len(addKeys) >0 {
				for _, key := range addKeys {
					conn.Send("SADD", key, oneAdsId)
					conn.Send("EXPIRE", key, ZK_ADS_CACHE_SINGLE_SET_KEY)					
				}
			}

		}		

	}

	// EXEC Transaction
	_, err = conn.Do("")
	if err != nil {
		log.Errorln("pipeline syncByCampaignId error: ", err)
		return err
	}

	log.Infof("== end sync worker id: %v , syncByCampaignId: %v ....", workerId, uuid)

	return nil
}



/*
	 uuid=广告主id
	 广告主id有变更时，同步广告主信息缓存
	 找广告需要用到广告主信息，等级，余额
*/

func syncByAdvertiserId(cache Cache, session *mgo.Session, uuid string, workerId int) error {

	log.Infof("== begin sync worker id: %v , SyncByAdvertiserId: %v ....", workerId, uuid)
	
	db := session.Copy()
	defer db.Close()
	

	var advertiser Advertiser
	c := db.DB("zk_dsp").C("advertiser")
	err := c.FindId(bson.M{
		"_id" : bson.ObjectIdHex(uuid),
	}).One(&advertiser)
	if err != nil {
		log.Error(err)
		return err
	}

	// Redis
	conn := cache.Pool.Get()
	defer conn.Close()

	advCacheKey := strings.Replace(ZK_DSP_CACHE_ADVERTISER_DATA, "{id}", "", 1)
	oneAdvJson, err := json.Marshal(advertiser)
	if err != nil {
		log.Warnln("Json format error advertiser_id:", uuid)
		return err
	}

	_, err = conn.Do("SETEX", strings.Join([]string{advCacheKey, advertiser.Id.Hex()}, ""), 86400, oneAdvJson)
	if err != nil {
		log.Errorln("Redis set advertiser fail advertiser_id:", uuid)
		return err
	}


	log.Infof("== end sync worker id: %v , SyncByAdvertiserId: %v ....", workerId, uuid)
	return nil
}


//批量查找广告信息
func getCampaignSpaceByIds(cache Cache , ids []string) ( map[string]CampaignSpace, error) {

	conn := cache.Pool.Get()
	defer conn.Close()	

	ads := map[string]CampaignSpace{}

	// Mget from redis
	var args []interface{}
	for _, id := range ids {
		args = append(args, strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, id}, ""))
	}
	values, err := redigo.Strings(conn.Do("MGET", args...))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// Json unmarshal
	for _, v := range values {
		cs := CampaignSpace{}
		err = json.Unmarshal([]byte(v), &cs)
		if err != nil {
			continue
		}

		ads[cs.Id.Hex()] = cs
	}

	return ads, nil
}


//加载mysql的category到缓存
func loadCategoryFromDbToCache(app *AppService)  {

	conn := app.cache.Pool.Get()
	defer conn.Close()

	db, err := sql.Open("mysql", app.mysqlconnectaddr)
    if err != nil {
		panic(err)
	}
	defer db.Close()
	
	results, err := db.Query("SELECT id, title FROM category")
	if err != nil {
		panic(err)
	}

	var args []interface{}
	args = append(args, ZK_ADS_CACHE_ALL_CATEGORY)
	
	for results.Next() {
		var category Category
		// for each row, scan the result into our tag composite object
		err = results.Scan(&category.Id, &category.Title)
		if err != nil {
			panic(err)
		}		
		args = append(args, category.Id, category.Title)
	}

	// log.Printf("== args: %v",args)
	
	//HMSET
	_, err = conn.Do("HMSET", args...)
	if err != nil {
		panic(err)
	}
	// Never expire
	conn.Do("PERSIST", ZK_ADS_CACHE_ALL_CATEGORY)


	log.Printf("== load category to redis successfully...")

	return

}


//加载third_party_id的广告到缓存
func loadThirdPartyFromDbToCache(app *AppService)  {

	//需要查询的第三方渠道ID
	thirdPartyIds := []string{
		"lenovobrowser_ads", 
		"meizu_flyme",
		"yizhuo",
		"91zhuomian",
		"zhongjingan",
		"JinliBrowserThirdads",
		"JinliWeatherThirdads",
		"JinliMusicThirdads",
		"JinliLockscreenThirdads",
		"jinlivideo",
		"jinlibaipai",
		"mucang",
		"go10086cn",
	}

	conn := app.cache.Pool.Get()
	defer conn.Close()

	log.Infof("== load thirdparty num: %v", len(thirdPartyIds))

	// 初始化mongodb
	// session, err := mgo.Dial(dbconnection)
	// if err != nil {
	// 	fmt.Println("cannot connect to :" + dbconnection)
	// 	panic(err)
	// }
	// defer session.Close()

	// session.SetMode(mgo.Monotonic, true)

	host := []string{
		app.dbconnectionthird,
	}
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: host,
		Direct: true,
		Timeout: 15 * time.Second,
	})
	if err != nil {
		fmt.Println("cannot connect to :" + app.dbconnectionthird)
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	defer session.Close()

	sessionCopy := session.Copy()
	defer sessionCopy.Close()


	// 搜索Mongodb
	c := sessionCopy.DB("zk_dsp").C("campaign_output")

	var thirdOutputs []ThirdOutput
	findErr := c.Find(bson.M{
		"type": "show",
		"deviceid": bson.M{"$in": thirdPartyIds},
	}).Select(bson.M{"deviceid":1, "campaignids":1}).All(&thirdOutputs)
	if findErr != nil {
		log.Println(findErr)
	}

	log.Infof("== find output record: %v", len(thirdOutputs) )
	
	// 保存到缓存	
	thirdkey := ZK_ADS_CACHE_THIRD_PARTY_OUTPUT_SET

	oneJson, err := json.Marshal(thirdOutputs)	
	if err != nil {
		log.Error("== failed encode json of third_party:", thirdkey)
	}		

	conn.Send("SET", thirdkey, oneJson)
	conn.Send("EXPIRE", thirdkey, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)
	log.Info("== ok set cache third_party_id: ", thirdkey)


	log.Info("== load third_party ad to redis successfully...")
	
	return
}

func getAdThirdPartyMapFromCache(cache Cache) (map[string][]string, error) {

	log.Info("== START AppService:  getAdThirdPartyFromCache ")

	conn := cache.Pool.Get()
	defer conn.Close()

	// 查询第三方mongo

	var thirdPartys []ThirdOutput
	
	values, err := redigo.String(conn.Do("GET", ZK_ADS_CACHE_THIRD_PARTY_OUTPUT_SET))
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(values), &thirdPartys)
	if err != nil {
		return nil, err
	}

	adThirdParty := map[string][]string{}
	
	for _, third := range thirdPartys {
		if len(third.Campaignids) > 0 {
			for _, adId := range third.Campaignids {
				if _, ok := adThirdParty[adId]; !ok {
					adThirdParty[adId] = []string{third.DeviceId}				
				} else {
					adThirdParty[adId] = append(adThirdParty[adId], third.DeviceId)					
				}
			}
		}
		
	}


	log.Info("== END AppService:  getAdThirdPartyFromCache")

	
	return adThirdParty, nil


}