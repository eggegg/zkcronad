package main

import (
	"log"
	"strings"
	"crypto/md5"
	"encoding/hex"	
)

const    (
	ZK_ADS_CACHE_ADS_SHOW_COUNT = "ads_cache_ads_show_count_"
) 

func main()  {
	log.Println("test...")

	adid := "5b02605ab09efefd74000007"

	key := adsCacheGetKey(strings.Join([]string{ZK_ADS_CACHE_ADS_SHOW_COUNT,adid},""), "as_", 16)

	log.Println(key)
}

// 获取zaker的redis加密key字符串
func adsCacheGetKey(key string, prefix string, length int)  string{
	md5Key := GetMD5Hash(key)
	log.Println("md5key:", md5Key)
	if length > 5 && length < 32 {
		md5Key = md5Key[0:length]
	} 
	log.Println(len(md5Key) , " cut to ",length, " : ",md5Key)
	return strings.Join([]string{prefix, md5Key}, "")
}

func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}