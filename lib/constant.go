package lib

import (
	"gopkg.in/mgo.v2/bson"
)

const(
    //平台>广告位>喜好分类  广告ID集合
    ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_FAV_ADS_SET = "ads_cache_device_group_fav_ads_set_"
    //平台>广告位>地域  广告ID集合
    ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_LOCATION_ADS_SET = "ads_cache_device_group_location_ads_set_"
    //平台>广告位>标签  广告ID集合
    ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_TAG_ADS_SET = "ads_cache_device_group_tag_ads_set_"
    //平台>广告位>频道  广告ID集合
    ZK_ADS_CACHE_SINGLE_DEVICE_SINGLE_GROUP_SINGLE_CHANNEL_ADS_SET = "ads_cache_device_group_channel_ads_set_"
    //单条广告信息
    ZK_ADS_CACHE_SINGLE_ADS_DEF  = "ads_cache_single_ads_def_"  
    //广告主数据缓存
    ZK_DSP_CACHE_ADVERTISER_DATA = "dsp_string_cache_advertiser_"
    //广告计划对应的创意信息
    ZK_ADS_CACHE_CAMPAIGN_CREATIVE = "ads_cache_campaign_creative_" 

    //广告曝光次数
    ZK_ADS_CACHE_ADS_SHOW_COUNT = "ads_cache_ads_show_count_"
    //广告点击次数
    ZK_ADS_CACHE_ADS_CLICK_COUNT = "ads_cache_ads_click_count_" 

    //某广告位的广告曝光次数
    ZK_ADS_CACHE_ADX_POSITION_SHOW_COUNT = "ads_cache_adx_position_show_count_"
    //某广告位的广告点击次数
    ZK_ADS_CACHE_ADX_POSITION_CLICK_COUNT = "ads_cache_adx_position_click_count_"
    //广告曝光数点击数的缓存时间
    ZK_ADS_ADS_CACHE_EXPIRE = 86400*20

    //ZAKER用户对广告曝光次数
    ZK_ADS_CACHE_USER_ADS_SHOW_COUNT = "ads_cache_user_ads_show_count_"
    //ZAKER用户对广告点击次数
    ZK_ADS_CACHE_USER_ADS_CLICK_COUNT = "ads_cache_user_ads_click_count_"

    //第三方ADX用户对广告曝光次数
    ZK_ADS_CACHE_ADX_USER_ADS_SHOW_COUNT = "ads_cache_adx_user_ads_show_count_"
    //第三方ADX用户对广告点击次数
    ZK_ADS_CACHE_ADX_USER_ADS_CLICK_COUNT = "ads_cache_adx_user_ads_click_count_"

    //合作渠道广告请求数
    ZK_ADS_CACHE_PARTNER_REQUEST_COUNT = "ads_cache_partner_request_count_"
    //合作渠道得到的广告响应数
    ZK_ADS_CACHE_PARTNER_RESPONSE_COUNT = "ads_cache_partner_response_count_"

    //广告给出某一出价的次数
    ZK_ADS_CACHE_PARTNER_AD_PRICE_BID_COUNT = "ads_cache_partner_ad_price_bid_count_"
    //广告某一出价竞标成功的次数
    ZK_ADS_CACHE_PARTNER_AD_PRICE_WIN_COUNT = "ads_cache_partner_ad_price_win_count_"

    //第三方ADX有关缓存的过期时间，7天
    ZK_ADS_CACHE_PARTNER_ADX_EXPIRE = 86400*7

    //广告是否过期
    ZK_ADS_CACHE_IS_AD_UNEXPIRED = "ads_cache_is_ad_unexpired_"           
    
    //全部广告集合ID
    ZK_ADS_CACHE_ALL_ADS_SET = "ads_cache_all_ads_set"

    //单挑广告信息的过期时间
    ZK_ADS_CACHE_SINGLE_ADS_EXPIRE = 3300

    ZK_ADS_CACHE_CREATIVE_SHOW_COUNT = "ads_cache_creative_show_count_"

    ZK_ADS_CACHE_CREATIVE_CLICK_COUNT = "ads_cache_creative_click_count_"


    //更新队列
    ZK_ADS_SYNC_CAMPAIGN_QUEUE   = "zk_ads_sync_campaign_queue"
    ZK_ADS_SYNC_CREATIVE_QUEUE   = "zk_ads_sync_creative_queue"
    ZK_ADS_SYNC_ADVERTISER_QUEUE = "zk_ads_sync_advertiser_queue" 
)


//广告创意
type Creative struct{
    Id bson.ObjectId `json:"_id" bson:"_id,omitempty"`   //创意ID
    Idm string `json:"id" bson:"id"`                              //创意ID
    Title string `json:"title" bson:"title"`                         //标题
    Ads_pic string `json:"ads_pic" bson:"ads_pic"`                     //大图
    Ads_short_pic string `json:"ads_short_pic" bson:"ads_short_pic"`         //小图
    Multi_pics []string `json:"multi_pics" bson:"multi_pics"`             //组图
    Aid string `json:"aid" bson:"aid"`                             //广告主ID
    Campaignid string `json:"campaignid" bson:"campaignid"`               //广告计划ID
    Ads_type int `json:"ads_type" bson:"ads_type"`                      //广告样式ID
    Status int `json:"status" bson:"status"`                          //状态
    Create_time int `json:"create_time" bson:"create_time"`                //创建时间
    Sort int `json:"sort" bson:"sort"`                              //排序位置
}

//广告信息
type Campaign struct {
    
    Id bson.ObjectId `json:"_id" bson:"_id,omitempty"`                                          //广告ID
    Ad_group_id string `json:"ad_group_id" bson:"ad_group_id"`                         //广告计划ID
    Ad_type int `json:"ad_type" bson:"ad_type"`                                    //广告类型
    Add_time int `json:"add_time" bson:"add_time"`                                  //创建时间
    Ads_content string `json:"ads_content" bson:"ads_content"`                         //广告内容
    Ads_group string `json:"ads_group" bson:"ads_group"`                             //广告位
    Ads_link_url string `json:"ads_link_url" bson:"ads_link_url"`                       //落地页链接
    Ads_name string `json:"ads_name" bson:"ads_name"`                               //广告名称
    Ads_pic string `json:"ads_pic" bson:"ads_pic"`                                 //广告大图
    Ads_short_pic string `json:"ads_short_pic" bson:"ads_short_pic"`                     //广告小图
    Ads_type int `json:"ads_type" bson:"ads_type"`                                  //广告样式
    Aid string `json:"aid" bson:"aid"`                                         //广告主ID
    Carrier []string `json:"carrier" bson:"carrier"`                               //定向运营商
    Category string `json:"category" bson:"category"`                               //广告分类
    Category_first string `json:"category_first" bson:"category_first"`                   //广告第一分类
    Category_second string `json:"category_second" bson:"category_second"`                 //广告第二分类
    Category_third string `json:"category_third" bson:"category_third"`                   //广告第三分类
    Channel []string `json:"channel" bson:"channel"`                               //定向频道
    CreativeId string `json:"creativeid" bson:"creativeid"`                           //广告创意ID

    Daily_target_views int `json:"daily_target_views" bson:"daily_target_views"`              //曝光日预算
    Daily_target_clicks int `json:"daily_target_clicks" bson:"daily_target_clicks"`            //点击日预算
    Deliver_speed_type int `json:"deliver_speed_type" bson:"deliver_speed_type"`              //投放速度，1：匀速投放，2：加速投放
    Deliver_time []string `json:"deliver_time" bson:"deliver_time"`                     //投放时间段
    Deliver_time_week []string `json:"deliver_time_week" bson:"deliver_time_week"`           //投放时间段(含星期几)
    Deliver_type int `json:"deliver_type" bson:"deliver_type"`                          //投放类型，1：CPC，2：CPM
    Device_type map[string]string `json:"device_type" bson:"device_type"`              //投放设备类型
    Disabled_partners []string `json:"disabled_partners" bson:"disabled_partners"`           //不投放的第三方渠道
    Disabled_position_ids []string `json:"disabled_position_ids" bson:"disabled_position_ids"`   //不投放的广告位ID
    Disabled_wap_partners []string `json:"disabled_wap_partners" bson:"disabled_wap_partners"`   //不投放的Wap渠道
    Start_time int `json:"start_time" bson:"start_time"`                              //开始时间
    End_time int `json:"end_time" bson:"end_time"`                                  //结束时间
    Favour_category []string `json:"favour_category" bson:"favour_category"`               //定向喜好分类
    Is_redirect int `json:"is_redirect" bson:"is_redirect"`                            //投放方式，0：智能投放，1：自定义
    Is_test int `json:"is_test" bson:"is_test"`                                    //是否是测试广告
    Loading_text string `json:"loading_text" bson:"loading_text"`                       //广告提示语
    Location []string `json:"location" bson:"location"`                             //定向地区
    Multi_pics []string `json:"multi_pics" bson:"multi_pics"`                         //组图
    Network_type []string `json:"network_type" bson:"network_type"`                     //定向网络类型

    Packageid string `json:"packageid" bson:"packageid"`                             //广告组ID
    Phone_brand []string `json:"phone_brand" bson:"phone_brand"`                       //定向设备品牌
    Price_times float32 `json:"price_times" bson:"price_times"`                        //广告价格加成倍数
    Priority string `json:"priority" bson:"priority"`                               //广告权重
    Prize_weight float64 `json:"prize_weight" bson:"prize_weight"`                      //广告出价
    Product string `json:"product" bson:"product"`                                 //广告产品
    Sex int `json:"sex" bson:"sex"`                                            //定向用户性别
    Sponsor string `json:"sponsor" bson:"sponsor"`                                 //广告主名称
    Stat int `json:"stat" bson:"stat"`                                          //广告状态
    Status int `json:"status" bson:"status"`                                      //广告计划状态
    Stitle string `json:"stitle" bson:"stitle"`                                   //副标题                    
    Tags []string `json:"tags" bson:"tags"`                                     //广告标签
    Target_clicks int `json:"target_clicks" bson:"target_clicks"`                        //点击总预算
    Target_views int `json:"target_views" bson:"target_views"`                          //曝光总预算
    Third_views_url string `json:"third_views_url" bson:"third_views_url"`                 //第三方曝光监测地址
    Tracker_url string `json:"tracker_url" bson:"tracker_url"`                         //第三方点击监测地址
    Web_target string `json:"web_target" bson:"web_target"`                           //打开连接方式，web或safari
}

type CampaignSpace struct {
	Campaign `bson:",inline"`

	Ad_group_def struct{       //广告计划信息
        Id string `json:"_id" bson:"_id"`
        Target_clicks int `json:"target_clicks" bson:"target_clicks"`            //点击总预算
        Daily_target_clicks int `json:"daily_target_clicks" bson:"daily_target_clicks"` //点击日预算
        Target_views int `json:"target_views" bson:"target_views"`              //曝光总预算
        Daily_target_views int `json:"daily_target_views" bson:"daily_target_views"`  //曝光日预算
    } `json:"ad_group_def" bson:"ad_group_def"`

    Creatives map[string]Creative `json:"creatives" bson:"creatives"`           //广告创意
}


type AdsStat struct {
    Ads_id string `bson:"ads_id"`
    Show_count int `bson:"showCount"`
    Click_count int `bons:"clickCount"`
    Daily_shows  map[string]int `bson:"daily_shows"`
    Daily_clicks map[string]int `bson:"daily_clicks"`
}


type CreativeStat struct {
    Creative_id string `bson:"creativeid"`
    Show_count int `bson:"showCount"`
    Click_count int `bons:"clickCount"`
    Daily_shows  map[string]int `bson:"daily_shows"`
    Daily_clicks map[string]int `bson:"daily_clicks"`
}

type Advertiser struct {
    Id bson.ObjectId `json:"_id" bson:"_id"`
    Balance int `json:"balance" bson:"balance"`
    Total_cost int `json:"total_cost" bson:"total_cost"`
    Grade string `json:"grade" bson:"grade"`
    Status int `json:"status" bson:"status"`
    Parentid string `json:"parentid" bson:"parentid"`
}