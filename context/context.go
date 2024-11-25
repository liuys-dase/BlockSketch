package context

import (
	"github.com/liuys-dase/csc-tree/config"
)

type Context struct {
	Config *config.ServerConfig
}

func NewContext(iniPath string) (*Context, error) {
	// 读取配置文件
	conf := config.NewServerConfig(iniPath)

	// 返回 Context 实例
	return &Context{
		Config: conf,
	}, nil
}

func NewContextOnlyConfig(iniPath string) (*Context, error) {
	// 读取配置文件
	conf := config.NewServerConfig(iniPath)

	// 返回 Context 实例
	return &Context{
		Config: conf,
	}, nil
}
