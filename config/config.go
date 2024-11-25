package config

import (
	"fmt"
	"log"

	"github.com/go-ini/ini"
)

type ServerConfig struct {
	CSCTreeConfig *CSCTreeConfig
}

func NewServerConfig(iniPath string) *ServerConfig {
	ini, err := readConfig(iniPath)
	if err != nil {
		log.Fatal("Error loading config:", err)
	}
	return &ServerConfig{
		CSCTreeConfig: NewCSCTreeConfig(ini),
	}
}

type CSCTreeConfig struct {
	MaxLevel            int
	BfFalsePositiveRate float64
	BfHashFuncNum       int
	FingerprintSize     int
	FingerprintNum      int
	MaxKickAttempts     int
	PartitionNum        int
	RepetitionNum       int
	MaxElementNumPerPar int
	SketchLevel         int
	UseNodeIndex        bool
	LeafNum             int
	UseFlatten          bool
}

func NewCSCTreeConfig(ini *ini.File) *CSCTreeConfig {
	return &CSCTreeConfig{
		MaxLevel:            ini.Section("CSCTree").Key("MaxLevel").MustInt(),
		BfFalsePositiveRate: ini.Section("CSCTree").Key("BfFalsePositiveRate").MustFloat64(),
		BfHashFuncNum:       ini.Section("CSCTree").Key("BfHashFuncNum").MustInt(),
		FingerprintSize:     ini.Section("CSCTree").Key("FingerprintSize").MustInt(),
		FingerprintNum:      ini.Section("CSCTree").Key("FingerprintNum").MustInt(),
		MaxKickAttempts:     ini.Section("CSCTree").Key("MaxKickAttempts").MustInt(),
		PartitionNum:        ini.Section("CSCTree").Key("PartitionNum").MustInt(),
		RepetitionNum:       ini.Section("CSCTree").Key("RepetitionNum").MustInt(),
		MaxElementNumPerPar: ini.Section("CSCTree").Key("MaxElementNumPerPar").MustInt(),
		SketchLevel:         ini.Section("CSCTree").Key("SketchLevel").MustInt(),
		UseNodeIndex:        ini.Section("CSCTree").Key("UseNodeIndex").MustBool(),
		LeafNum:             ini.Section("CSCTree").Key("LeafNum").MustInt(),
		UseFlatten:          ini.Section("CSCTree").Key("UseFlatten").MustBool(),
	}
}

// 读取配置文件
func readConfig(filePath string) (*ini.File, error) {
	ini, err := ini.Load(filePath)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}
	return ini, nil
}
