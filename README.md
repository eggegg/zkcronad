# zkcronad


docker run --name ad-redis -p 6379:6379 -d redis
docker run --name ad-cache-redis -p 6380:6379 -d redis
docker run --name ad-mongo -p 27017:27017 -d mongo

停用过期广告
refreshCreativeCache 
没有实施更新缓存，需要每五分钟运行一次脚本来更新