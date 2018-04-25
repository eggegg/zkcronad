package lib

import (
	"log"
	mgo "gopkg.in/mgo.v2"
	redigo "github.com/garyburd/redigo/redis"	
)

type Worker struct {
	cache Cache
	session *mgo.Session
	id    int
	queue string
}


func newWorker(cache Cache, session *mgo.Session, id int, queue string) Worker {
	return Worker{cache: cache, session: session, id: id, queue: queue}
}



func (w Worker) processAdvertiser(id int) {
	for {
		conn := w.cache.Pool.Get()
		var channel string
		var uuid int
		if reply, err := redigo.Values(conn.Do("BLPOP", w.queue,30+id)); err == nil {

			if _, err := redigo.Scan(reply, &channel, &uuid); err != nil {
				w.cache.enqueueValue(w.queue, uuid)
				continue
			}

			values, err := redigo.String(conn.Do("GET", uuid))
			if err != nil {
				w.cache.enqueueValue(w.queue, uuid)
				continue
			}

			if err := syncByAdvertiserId(w.cache, w.session, values, w.id); err != nil {
				w.cache.enqueueValue(w.queue, uuid)
				continue
			}

		} else if err != redigo.ErrNil {
			log.Fatal(err)
		}
		conn.Close()
	}
}