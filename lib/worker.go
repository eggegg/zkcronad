package lib

import (
	_ "time"
	"log"
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
		log.Printf("worker(%v) defer..", id)
	}()

	log.Printf("worker(%v) start working...", id)

	for {
		conn := w.cache.Pool.Get()
		var channel string
		var uuid string

		log.Printf("worker(%v) start check redis: %v", id ,w.queue)
		if reply, err := redigo.Values(conn.Do("BLPOP", w.queue,30+id)); err == nil {
			if _, err := redigo.Scan(reply, &channel, &uuid); err != nil {
				w.cache.enqueueValue(w.queue, uuid)
				continue
			}

			log.Printf("worker(%v) get queue: %v", id ,uuid)
			// values, err := redigo.String(conn.Do("GET", uuid))
			// if err != nil {
			// 	w.cache.enqueueValue(w.queue, uuid)
			// 	continue
			// }

			// if err := syncByAdvertiserId(w.cache, w.session, values, w.id); err != nil {
			// 	w.cache.enqueueValue(w.queue, uuid)
			// 	continue
			// }

		} else if err != redigo.ErrNil {
			log.Fatal(err)
		}
		conn.Close()
		
		// log.Printf("worker(%v) is running..", id)
		// time.Sleep(2 * time.Second)

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