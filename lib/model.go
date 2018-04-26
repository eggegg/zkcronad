package lib

import (
	"time"
	log "github.com/Sirupsen/logrus"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"encoding/json"

	"strings"

	redigo "github.com/garyburd/redigo/redis"

	"errors"
	
)



func getDataFromMongo(app *AppService)  {
	log.Println("== Beginning search mongodb....")

	session := app.session.Copy()
	defer session.Close()

	// Redis
	conn := app.cache.Pool.Get()
	defer conn.Close() // 关闭cache的redis连接
	
	// Begin Redis Transaction
	conn.Send("MULTI")

	// Get Campaign Space
	c := session.DB("zk_dsp").C("campaign_space")
	var campaignSpaces []CampaignSpace
	
	nowTimeStamp := time.Now().Unix()
	if app.mode == "development" {
		nowTimeStamp = 1523849239
	}

	// BEGIN 删除状态不正常的广告缓存
	var  unNormalAid []struct {
		Id bson.ObjectId `bson:"_id"`
	}

	unNormalErr := c.Find(bson.M{
		"start_time": bson.M{"$lt": nowTimeStamp},
		"end_time": bson.M{"$gt": nowTimeStamp},
		"stat" : bson.M{"$ne" : 1},
	}).Select(bson.M{"_id":1}).All(&unNormalAid)
	if unNormalErr != nil {
		log.Errorf("== err searching unnormal ad: %v ", unNormalErr)
	}
	log.Infof("== find unnormalad num : %v", len(unNormalAid) )
	
	if len(unNormalAid) > 0 {
		for _, v := range unNormalAid {
			conn.Send("DEL", strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, v.Id.Hex()}, ""))			
		}
	}
	// END 删除状态不正常的广告缓存 




	beginTimeStamp := time.Now()
	err := c.Find(bson.M{
		"start_time": bson.M{"$lt": nowTimeStamp},
		"end_time": bson.M{"$gt": nowTimeStamp},
		"stat" : 1,
		"no_balance" : bson.M{"$ne" : 1},
	}).All(&campaignSpaces)

	if err != nil {
		log.Println("== Failed get campaign:", err)
	}
	log.Println("== numbers of campaign_space: ", len(campaignSpaces), ", using :", time.Since(beginTimeStamp));
	
	var allCampaignIds []string
	for _, v := range campaignSpaces {
		allCampaignIds = append(allCampaignIds, v.Ad_group_id)
	} 
	log.Println("== number of all campaign of all the campaign_space: ", len(allCampaignIds) )

	// for v := range allCampaignIds {
	// 	log.Printf("== %v, %T", v, v)
	// 	// log.Printf("== all : $T, %T", k ,v)		
	// }
	campaignIds := getUniqueString(allCampaignIds)
	log.Println("== number of unique campaign of all the campaign_space: ", len(campaignIds))

	var campaignBsonIds []bson.ObjectId
	for _, id := range campaignIds {
		campaignBsonIds = append(campaignBsonIds, bson.ObjectIdHex(id))
	}
	
	// Get Campaign 

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

	log.Println("== step2 == numbers of campaign: ", len(campaigns), ", using :", time.Since(beginTimeStamp))


	// Get Creative
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

	// for key, value := range creativesMap {
	// 	log.Printf("== [%v,%T] length(%v), %v,%T", key, key, len(value), value, value)
	// }

	log.Println("== step3 == numbers of creatives: ", len(creatives), ", using:", time.Since(beginTimeStamp))


	/*
		每条广告设置缓存
		全部广告id设置缓存
		广告id按属性设置缓存
	*/

	var oneAdsId, oneAdsCampaignId string
	var allAdsId []string
	// var oneAdsCampaign Campaign

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

		//加入全部广告ID数组
		allAdsId = append(allAdsId, oneAdsId)

		//创意
		// log.Infof("== find creatives of : %v", oneAdsCampaignId)
		oneCreatives, ok := creativesMap[oneAdsCampaignId]
		if ok == true {
			// log.Infof("== find creative: %v, %T, %v", oneCreatives, oneCreatives, len(oneCreatives))
			oneAds.Creatives = oneCreatives
		} else {
			//没有广告创意的不加载
			continue
		}

		//58同城的广告集合
		//将广告所属小类加入广告标签

		//设置单条缓存和过期时间
		oneAdsJson, err := json.Marshal(oneAds)	
		if err != nil {
			log.Error("== failed encode json of campaign_id:", oneAdsId)
		}			
		oneAdsRedisKey := strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, "")
		conn.Send("SET", oneAdsRedisKey, oneAdsJson)
		conn.Send("EXPIRE", oneAdsRedisKey, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)

		//按条件设置缓存
		setKeys := getPreloadRedisKeys(&oneAds)
		// log.Infof("== >>> get setkeys of :%v, len(%v)", oneAdsId, len(setKeys))

		for _, value := range setKeys {
			_, ok := adsGroupMap[value] 
			if !ok {
				adsGroupMap[value] = true
				adsGroup[value] = []string{oneAdsId}
			} else {
				adsGroup[value] = append(adsGroup[value], oneAdsId)
			}
		}

		// log.Printf("== oneAds info : %v, %T", oneAds, oneAds)
	}

	log.Println("=== finish adsGroup : %v, %v", len(adsGroup), len(adsGroupMap))

	// for _, value := range adsGroup {
	// 	log.Printf("== key for the : %v", value)
	// }


	//先删除再设置全部广告ID缓存
	conn.Send("DEL", ZK_ADS_CACHE_ALL_ADS_SET)
	for _, id := range allAdsId {
		conn.Send("SADD", ZK_ADS_CACHE_ALL_ADS_SET, id)
	}
	conn.Send("EXPIRE", ZK_ADS_CACHE_ALL_ADS_SET, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)


	//处理广告属性的redis集合设置
	for key, _ := range adsGroupMap {
		conn.Send("DEL", key)
	}
	for key, value := range adsGroup {
		for _, id := range value {
			conn.Send("SADD", key, id)
		}
		conn.Send("EXPIRE", key, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)
	}

	_, err = conn.Do("EXEC")
	if err != nil {
		log.Error("== redis multi exec error: ", err)
	}
	log.Printf("== redis multi exec command finished" )



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
	adsConn.Send("MULTI")

	//更新广告的曝光数和点击数
	c = session.DB("zk_ads_stat").C("ads_action_stat")
	
	var adsStats []AdsStat
	err = c.Find(bson.M{
		"ads_id" : bson.M{"$in": allAdsId},
	}).All(&adsStats)
	if err != nil {
		log.Error(err)
	}
	log.Infof("== get ads stat: %v", len(adsStats))

	if len(adsStats) > 0 {
		for _, stat := range adsStats {
			if stat.Ads_id == "" {
				continue
			}
			if stat.Show_count > 0 {
				showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id},""), "as_", 16)
				adsConn.Send("SET", showCacheKey, stat.Show_count)
				adsConn.Send("EXPIRE", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
			}
			if stat.Click_count > 0 {
				clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id},""), "ac_", 16)
				adsConn.Send("SET", clickCacheKey, stat.Click_count)
				adsConn.Send("EXPIRE", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
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
	log.Infof("== get campaign stat: %v", len(adsCampaignStats))

	todayDateFormat := time.Now().Format("2006-01-02")

	if len(adsCampaignStats) > 0 {
		for _, stat := range adsCampaignStats {
			if stat.Ads_id == "" {
				continue
			}
			if stat.Show_count > 0 {
				showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id},""), "as_", 16)
				adsConn.Send("SET", showCacheKey, stat.Show_count)
				adsConn.Send("EXPIRE", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
			}
			if stat.Click_count > 0 {
				clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id},""), "ac_", 16)
				adsConn.Send("SET", clickCacheKey, stat.Click_count)
				adsConn.Send("EXPIRE", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
			}

			if len(stat.Daily_shows) > 0 {
				todayShow, ok := stat.Daily_shows[todayDateFormat]
				if ok == true && todayShow > 0 {
					showCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,stat.Ads_id,"_",todayDateFormat}, ""), "as_", 16)
					adsConn.Send("SET", showCacheKey, todayShow)
					adsConn.Send("EXPIRE", showCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
				}
			}
			if len(stat.Daily_clicks) > 0 {
				todayClick, ok := stat.Daily_clicks[todayDateFormat]
				if ok == true && todayClick > 0 {
					clickCacheKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_CLICK_COUNT,stat.Ads_id,"_",todayDateFormat}, ""), "ac_", 16)				
					adsConn.Send("SET", clickCacheKey, todayClick)
					adsConn.Send("EXPIRE", clickCacheKey, ZK_ADS_ADS_CACHE_EXPIRE)
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
	log.Infof("== get creatives stat: %v", len(creativeStats))

	if len(creativeStats) > 0 {

		var today, yesterday, dayBeforeYesterday string

		if app.mode == "development" {
			//测试模式使用测试数据
			name, offset := time.Now().Zone()
			log.Println("find timezone:", name, offset)
			currenttime := time.Unix(nowTimeStamp+int64(offset), 0)
			today = currenttime.Format("2006-01-02")
			yesterday = currenttime.AddDate(0, 0, -1).Format("2006-01-02")
			dayBeforeYesterday = currenttime.AddDate(0,0,-2).Format("2006-01-02")
		} else {
			today = time.Now().Format("2006-01-02")		
			yesterday = time.Now().AddDate(0, 0, -1).Format("2006-01-02")
			dayBeforeYesterday = time.Now().AddDate(0,0,-2).Format("2006-01-02")
		}

		
		log.Printf("today: %v, yesterday: %v, dayBeforeYesterday:%v", today, yesterday, dayBeforeYesterday)
		dayKeys := []string{today, yesterday, dayBeforeYesterday}

		for _, stat := range creativeStats {
			creativeShowKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_SHOW_COUNT, stat.Creative_id}, ""), "cs_", 16)
			creativeClickKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_CLICK_COUNT, stat.Creative_id}, ""), "cc_", 16)

			adsConn.Send("SETEX", creativeShowKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Show_count)
			adsConn.Send("SETEX", creativeClickKey, ZK_ADS_ADS_CACHE_EXPIRE, stat.Click_count)

			
			if len(stat.Daily_shows) > 0 {
				// log.Infof("== %v, daily_show: %v", stat.Creative_id,stat.Daily_shows)
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
						dailyClickKey := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_CREATIVE_CLICK_COUNT}, ""), "cs_", 16)
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
		// log.Printf("== adv: %v, %T", adv, adv)
		oneAdvJson, err := json.Marshal(adv)	
		if err != nil {
			log.Error("== failed encode json of advtiser:", adv.Id.Hex())
		}		
		adsConn.Send("SETEX", strings.Join([]string{advCacheKey, adv.Id.Hex()}, ""), 86400, oneAdvJson)
	}
	

	// 批量执行
	_, err = adsConn.Do("EXEC")
	if err != nil {
		log.Error("== redis multi exec error: ", err)
	}
	log.Printf("== redis multi exec command finished" )

	log.Printf("== all reload tasks finished successfully")

}



// 查找某一个广告所有的属性组合key
func getPreloadRedisKeys(ad *CampaignSpace) []string {
	setKeys := []string{}

	adGroup := ad.Ads_group

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
	// log.Printf("== get campaign: %v", campaign)

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
	log.Printf("== get old campaignspace num: %v", len(oldCampaignSpaceMap) )
	
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
	

	// Begin Redis Transaction
	conn.Send("MULTI")

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

		//设置单条缓存和过期时间
		oneAdsJson, err := json.Marshal(oneAds)	
		if err != nil {
			log.Error("== failed encode json of campaign_id:", oneAdsId)
		}			
		oneAdsRedisKey := strings.Join([]string{ZK_ADS_CACHE_SINGLE_ADS_DEF, oneAdsId}, "")			

		conn.Send("SET", oneAdsRedisKey, oneAdsJson)
		conn.Send("EXPIRE", oneAdsRedisKey, ZK_ADS_CACHE_SINGLE_ADS_EXPIRE)


		//对比旧的数据和新的数据，更新对应的属性
		oldCampaignSpace, ok := oldCampaignSpaceMap[oneAdsId]
		if ok != true {
			continue
		}

		oldSetKeys := getPreloadRedisKeys(&oldCampaignSpace)
		newSetKeys := getPreloadRedisKeys(&oneAds) 
		log.Infof("== deletekey num: %v, addkey num: %v", len(oldSetKeys), len(newSetKeys))
	

		deleteKeys := differentOfSlicesString(oldSetKeys, newSetKeys)
		addKeys := differentOfSlicesString(newSetKeys, oldSetKeys)
		log.Infof("== deletekey num: %v, addkey num :  %v", len(deleteKeys), len(addKeys))


		if len(deleteKeys) > 0 {
			for _, key := range deleteKeys {
				conn.Send("SREM", key, oneAdsId)
			}
		}
		if len(addKeys) >0 {
			for _, key := range addKeys {
				conn.Send("SADD", key, oneAdsId)
			}
		}
	

	}

	// EXEC Transaction
	_, err = conn.Do("EXEC")
	if err != nil {
		log.Error("== redis multi exec error: ", err)
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

	_, err = conn.Do("SETEX", strings.Join([]string{advCacheKey, advertiser.Id.Hex()}, ""), 86400, oneAdvJson)
	if err != nil {
		log.Error(err)
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