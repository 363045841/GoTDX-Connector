// 服务配置：所有环境变量的唯一读取点，装配阶段由 main 调用一次。
package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config tdx-api 的完整运行配置。
type Config struct {
	Port           string
	SymbolDBPath   string
	LiveInterval   time.Duration
	LiveMaxStreams int
	LiveMaxSeries  int
	AutoSelect     bool
	MainHosts      []string
	ExHosts        []string
	MACHosts       []string
}

// Load 从环境变量读取配置并应用与文档一致的默认值。
func Load() Config {
	return Config{
		Port:           envString("PORT", "8080"),
		SymbolDBPath:   envString("SYMBOL_DB_PATH", filepath.Join("data", "tdx-symbols.db")),
		LiveInterval:   envDuration("GOTDX_LIVE_POLL_INTERVAL", 5*time.Second, 500*time.Millisecond),
		LiveMaxStreams: envInt("GOTDX_LIVE_MAX_STREAMS", 256),
		LiveMaxSeries:  envInt("GOTDX_LIVE_MAX_SERIES", 32),
		AutoSelect:     os.Getenv("GOTDX_AUTO_SELECT") == "1",
		MainHosts:      envList("GOTDX_MAIN_HOSTS"),
		ExHosts:        envList("GOTDX_EX_HOSTS"),
		MACHosts:       envList("GOTDX_MAC_HOSTS"),
	}
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback, minimum time.Duration) time.Duration {
	parsed, err := time.ParseDuration(os.Getenv(key))
	if err != nil || parsed < minimum {
		return fallback
	}
	return parsed
}

func envInt(key string, fallback int) int {
	parsed, err := strconv.Atoi(os.Getenv(key))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envList(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}
