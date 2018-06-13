package main

import (
	"time"
	"fmt"

	mgo "gopkg.in/mgo.v2"
)

const (
	DBCONNECT = "192.168.88.20:27017"
)

func main()  {
	fmt.Println("begin search mongodb")

	host := []string{
		DBCONNECT,
	}
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: host,
		Direct: true,
		Timeout: 10 * time.Second,
	})
	fmt.Printf("get session:%T", session)
	// session, err := mgo.Dial(DBCONNECT)
	if err != nil {
		fmt.Println("cannot connect to :" + DBCONNECT)
		panic(err)
	}
	session.SetMode(mgo.Strong, true)	
	defer session.Close()

	// sessionCopy := session.Copy()
	// defer sessionCopy.Close()

	collection := session.DB("zk_dsp").C("campaign_output")
	// collection := session.DB("zk_dsp").C("campaign_output")
	
	fmt.Printf("== collection: %v", collection)
	countNum, err := collection.Count()
	if err != nil {
		fmt.Println("cannot search collection")
		fmt.Println(err)
	}
	fmt.Println("collection num count:", countNum)
}