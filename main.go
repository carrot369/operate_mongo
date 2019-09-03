package main

import (
	"flag"
	"github.com/cihub/seelog"
	"operate_mongo/config"
	"operate_mongo/mongodb"
)

func main(){
	InitLog()

	confFile := flag.String("confFile", "config/operate_mongo.yml", "Configuration file.")
	flag.Parse()

	config.LoadConfig(*confFile)

	if config.GetConfig().IsInsert == 1 {
		mongodb.InsertSpecifiedDocs()
	} else {
		mongodb.DeleteSpecifiedDocs()
	}
}

func InitLog(){
	defer seelog.Flush()

	//加载配置文件
	logger, err := seelog.LoggerFromConfigAsFile("config/log_config.xml")

	if err!=nil{
		panic("parse log_config.xml error")
	}

	//替换记录器
	seelog.ReplaceLogger(logger)

	seelog.Info("InitLog")
}