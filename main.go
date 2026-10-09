package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	var target string
	switch os.Args[1] {
	case "tdx", "tdx-api", "gotdx":
		target = "./services/tdx-api"
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown service: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}

	cmd := exec.Command("go", append([]string{"run", target}, os.Args[2:]...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "failed to run %s: %v\n", target, err)
		os.Exit(1)
	}
}

func printUsage() {
	exe := filepath.Base(os.Args[0])
	if exe == "main" || exe == "main.exe" {
		exe = "go run ."
	}
	fmt.Fprintf(os.Stderr, `KlineChartQuantGo — multi-service launcher

Usage:
  %s <service>

Services:
  tdx, tdx-api, gotdx     Start TDX/gotdx API (port 8080)

Examples:
  go run . tdx
  go run ./services/tdx-api
`, exe)
}
