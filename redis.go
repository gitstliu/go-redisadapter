package redisadapter

import (
	"common"
	"time"

	"github.com/alecthomas/log4go"
	"github.com/garyburd/redigo/redis"
)

type RedisPipelineCommand struct {
	CommandName string
	Key         string
	Args        []interface{}
}

var adapter = RedisAdapter{}

func GetAdapter() *RedisAdapter {
	return &adapter
}

type RedisAdapter struct {
}

var redisClient *redis.Pool

func CreadRedisPool(host string, password string, maxIdle int, maxActive int, idleTimeout time.Duration, db int) {
	redisClient = &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout * time.Second,
		Dial: func() (redis.Conn, error) {

			var c redis.Conn
			var err error
			if password != "" {
				option := redis.DialPassword(password)
				c, err = redis.Dial("tcp", host, option)
			} else {
				c, err = redis.Dial("tcp", host)
			}

			if err != nil {
				return nil, err
			}
			c.Do("SELECT", db)
			return c, nil
		},
	}
}

func (this *RedisAdapter) SET(key, value string) (string, error) {
	log4go.Debug("key is %v", key)
	client := redisClient.Get()
	defer client.Close()
	return redis.String(client.Do("SET", key, value))
}

func (this *RedisAdapter) GET(key string) (string, error) {
	log4go.Debug("key is %v", key)
	client := redisClient.Get()
	defer client.Close()
	return redis.String(client.Do("GET", key))
}

func (this *RedisAdapter) KEYS(key string) ([]string, error) {
	log4go.Debug("key is %v", key)
	client := redisClient.Get()
	defer client.Close()
	return redis.Strings(client.Do("KEYS", key))
}

func (this *RedisAdapter) LPUSH(key string, value []interface{}) (interface{}, error) {
	log4go.Debug("key is %v, value is %v", key, value)
	client := redisClient.Get()
	defer client.Close()
	return client.Do("LPUSH", append([](interface{}){key}, value...)...)
}

func (this *RedisAdapter) RPUSH(key string, value []interface{}) (interface{}, error) {
	log4go.Debug("key is %v, value is %v", key, value)
	defer common.PanicHandler()
	client := redisClient.Get()
	defer client.Close()
	log4go.Debug(append([](interface{}){key}, value...))
	result, err := client.Do("RPUSH", append([](interface{}){key}, value...)...)
	log4go.Debug("err is %v, key is %v, value is %v", err, key, result)
	return result, err
}

func (this *RedisAdapter) LPOP(key string) (string, error) {
	client := redisClient.Get()
	defer client.Close()
	result, err := redis.String(client.Do("LPOP", key))
	log4go.Debug("err is %v, key is %v, value is %v", err, key, result)
	return result, err
}

func (this *RedisAdapter) LRANGE(key string, index int, endIndex int) ([]string, error) {
	client := redisClient.Get()
	defer client.Close()
	result, err := redis.Strings(client.Do("LRANGE", key, index, endIndex))
	log4go.Debug("err is %v, key is %v, value is %v", err, key, result)
	return result, err
}

func (this *RedisAdapter) SendPipelineCommands(commands []RedisPipelineCommand) ([]interface{}, []error) {
	log4go.Debug("commands %v", commands)
	errorList := make([]error, 0, len(commands)+1)

	client := redisClient.Get()
	defer client.Close()
	for index, value := range commands {
		log4go.Debug("Curr Commands index is %v value is %v", index, value)
		log4go.Debug("********************")
		log4go.Debug("%v", [](interface{}){value.Key})

		for in, v := range value.Args {
			log4go.Debug("===== %v %v", in, v)
		}

		log4go.Debug("%v", value.Args...)
		log4go.Debug("%v", append([](interface{}){value.Key}, value.Args...))
		log4go.Debug("%v", append([](interface{}){value.Key}, value.Args...)...)
		currErr := client.Send(value.CommandName, append([](interface{}){value.Key}, value.Args...)...)

		if currErr != nil {
			errorList = append(errorList, currErr)
		}
	}

	log4go.Debug("Send finished!!")

	fulshErr := client.Flush()

	if fulshErr != nil {
		errorList = append(errorList, fulshErr)

		return nil, errorList
	}

	replys := [](interface{}){}

	replysLength := len(commands)

	for i := 0; i < replysLength; i++ {
		reply, receiveErr := client.Receive()

		if receiveErr != nil {
			errorList = append(errorList, receiveErr)
		}

		replys = append(replys, reply)
	}

	log4go.Debug("Receive finished!!")

	if len(errorList) != 0 {
		return replys, errorList
	}

	return replys, nil
}
