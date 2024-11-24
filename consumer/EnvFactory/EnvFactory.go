package EnvFactory

import (
	"log"
	"github.com/spf13/viper"
)

var (
	envReader *viper.Viper
)

func NewEnvFactory(ConfigPath string) error {
	viper.SetConfigFile(ConfigPath)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}
	envReader = viper.GetViper()
	return nil
}

func GetStringValue(key string) string {
	result := envReader.GetString(key)
	return result
}

func SetStringValue(key string, value string) {
	envReader.Set(key, value)
}