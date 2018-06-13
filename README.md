# zkcronad


docker run --name ad-redis -p 6379:6379 -d redis
docker run --name ad-cache-redis -p 6380:6379 -d redis
docker run --name ad-mongo -p 27017:27017 -d mongo

停用过期广告
refreshCreativeCache 
没有实施更新缓存，需要每五分钟运行一次脚本来更新

CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o zkcronad .

dbconnection: "192.168.10.10:27017"
dbconnectionthird: "192.168.10.10:27017"
rediscacheaddress: "127.0.0.1:6379"
rediscacheaddress2: "127.0.0.1:6380"
mysqlconnectaddr: "homestead:secret@tcp(127.0.0.1:33060)/zk_dsp"
workernum: 1
pid: adcron.pid


dbconnection: "192.168.10.10:27017"
dbconnectionthird: "192.168.10.10:27017"
rediscacheaddress: "192.168.9.23:6379"
rediscacheaddress2: "192.168.9.23:6379"
mysqlconnectaddr: "production:3GCTfWYbP0KV@tcp(192.168.11.1:3306)/poly"
workernum: 1
pid: adcron.pid