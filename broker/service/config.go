package service

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v1"
)

type Config struct {
	Common struct {
		Version  string
		IsDebug  bool `yaml:"debug"`
		LogPath  string
		LogLevel string
	}
	Broker struct {
		Host    string
		TcpPort string
		WsPort  string
	}

	Store struct {
		Engine string
		FDB    struct {
			Namespace string
			Threads   int
		}
	}

	Cluster struct {
		HwAddr    string
		Port      string
		SeedPeers []string
	}

	Admin struct {
		Addr  string
		Token string
	}
}

func initConfig(path string) *Config {
	conf := &Config{}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal("read config error :", err)
	}

	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal("yaml decode error :", err)
	}

	return conf
}
