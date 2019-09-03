package config

import (
	"github.com/cihub/seelog"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

var g_config *config

type config struct {
	HostPort   string      `yaml:"hostport"`
	MongoInsert mongoInsert `yaml:"mongo_insert"`
	MongoDel   mongoDel    `yaml:"mongo_del"`
	IsInsert   int         `yaml:"is_insert"`
}

type mongoInsert struct {
	DataBase    string `yaml:"db"`
	Collection  string `yaml:"collection"`
	InsertCount int `yaml:"insertCount"`
	Field1      field1 `yaml:"field1"`
	Field2      field2 `yaml:"field2"`
}

type field1 struct {
	Key   string `yaml:"key"`
	Value int    `yaml:"value"`
}

type field2 struct {
	Key   string `yaml:"key"`
	Value int    `yaml:"value"`
}

type mongoDel struct {
	DataBase   string `yaml:"db"`
	Collection string `yaml:"collection"`
	Days       int    `yaml:"days"`
	TimeKey    string `yaml:"timekey"`
	Field      field  `yaml:"field"`
}

type field struct {
	Key   string `yaml:"key"`
	Value int    `yaml:"value"`
}

func LoadConfig(filename string) (conf *config) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		seelog.Error(err)
		return nil
	}

	// Expand env vars
	data = []byte(os.ExpandEnv(string(data)))

	// Decoding config
	if err = yaml.UnmarshalStrict(data, &conf); err != nil {
		seelog.Error(err)
		return nil
	}

	g_config = conf

	seelog.Infof("LoadConfig: %v", *conf)
	return
}

func GetConfig() *config {
	if g_config == nil {
		seelog.Error("CONFIG FILE IS NULL!")
	}
	return g_config
}
