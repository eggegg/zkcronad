package main

import (
	"crypto/md5"
	"encoding/hex"	
	"strings"
)



// 字符串slice排重
func getUniqueString(input []string) []string {
	u := make([]string, 0, len(input))
	m := make(map[string]bool)

	for _, val := range input {
		if _, ok := m[val]; !ok {
			m[val] = true
			u = append(u, val)
		}
	}

	return u
}

// 判断slice是否包含string
func stringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

// 获取zaker的redis加密key字符串
func adsCacheGetKey(key string, prefix string, length int)  string{
	md5Key := GetMD5Hash(key)
	if length > 5 && length < 32 {
		md5Key = md5Key[0:length]
	} 
	return strings.Join([]string{prefix, md5Key}, "")
}

func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

// difference returns the elements in a that aren't in b
func differentOfSlicesString(a, b []string) []string {
	mb := map[string]bool{}
    for _, x := range b {
        mb[x] = true
    }
    ab := []string{}
    for _, x := range a {
        if _, ok := mb[x]; !ok {
            ab = append(ab, x)
        }
    }
    return ab
}