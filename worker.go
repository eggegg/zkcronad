package main

import (
	_ "time"
	log "github.com/Sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	redigo "github.com/garyburd/redigo/redis"	

	"sync"
)

type Worker struct {
	cancelChan <-chan struct{}
	cache Cache
	session *mgo.Session
	id    int
	queue string
}


func newWorker(cancelChan <-chan struct{}, cache Cache, session *mgo.Session, id int, queue string) Worker {
	return Worker{cancelChan:cancelChan, cache: cache, session: session, id: id, queue: queue}
}


func (w Worker) process(id int) {
	defer func ()  {
		log.Infof("worker %v(%v) defer ending  ...", w.queue,id)
	}()

	log.Infof("worker %v(%v) start working...", w.queue, id)

	for {
		conn := w.cache.Pool.Get()
		var channel string
		var uuid string

		// log.Printf("worker %v(%v) start check redis", w.queue, id)
		if reply, err := redigo.Values(conn.Do("BLPOP", w.queue,30+id)); err == nil {
			if _, err := redigo.Scan(reply, &channel, &uuid); err != nil {
				w.cache.enqueueValue(w.queue, uuid)
				continue
			}

			// log.Infof("worker %v(%v) get queue: %v",w.queue, id ,uuid)

			switch w.queue {
				//同步广告计划缓存
				case ZK_ADS_SYNC_CAMPAIGN_QUEUE:
					log.Info("-- ZK_ADS_SYNC_CAMPAIGN_QUEUE")
					if err := syncByCampaignId(w.cache, w.session, uuid, w.id); err != nil {
						w.cache.enqueueValue(w.queue, uuid)
						continue
					}

				//同步广告主缓存
				case  ZK_ADS_SYNC_ADVERTISER_QUEUE:
					log.Info("-- ZK_ADS_SYNC_ADVERTISER_QUEUE")
					if err := syncByAdvertiserId(w.cache, w.session, uuid, w.id); err != nil {
						w.cache.enqueueValue(w.queue, uuid)
						continue
					}
			}
			


		} else if err != redigo.ErrNil {
			log.Fatal(err)
		}
		conn.Close()
		

		// Listen for the cancel channel
		select {
			case <- w.cancelChan:
				return
			default:
		}
		
	}
}


func SyncAdJob(cancelChan  <-chan struct{}, numWorkers int, cache Cache, session *mgo.Session, queue string)  {
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(cancelChan <-chan struct{}, id int, cache Cache, session *mgo.Session, queue string){
			worker := newWorker(cancelChan, cache, session, id, queue)
			worker.process(id)
			defer wg.Done()
		}(cancelChan,i,cache,session,queue)
	}
	wg.Wait()
}