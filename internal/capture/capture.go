package capture

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"home/mikannse/netproc/ecapGrpc/internal/logger"
	"home/mikannse/netproc/ecapGrpc/internal/utils"
)

const (
	maxHeaderLines = 200
)

func RunCapture(password string) (*exec.Cmd, io.ReadCloser, error) {
	cmd := exec.Command("sudo", "-S", "./ecapture", "tls")
	cmd.Stdin = bytes.NewBufferString(password + "\n")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("标准输出管道错误: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("标准错误管道错误: %w", err)
	}

	// 错误日志处理
	go func() {
		scanner := utils.NewScanner(stderr)
		for scanner.Scan() {
			logger.Error("[ECAPTURE] %s", scanner.Text())
		}
	}()

	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("启动命令失败: %w", err)
	}

	return cmd, stdout, nil
}

func TerminateEcapture(cmd *exec.Cmd, password string) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	// 使用进程树终止
	killCmd := exec.Command("sudo", "-S", "pkill", "-P", fmt.Sprintf("%d", cmd.Process.Pid))
	killCmd.Stdin = bytes.NewBufferString(password + "\n")

	if err := killCmd.Run(); err != nil {
		return fmt.Errorf("终止进程失败: %w", err)
	}

	return cmd.Wait()
}

func SkipInitialOutput(r utils.BufferedReader) error {
	skipCount := 0
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == utils.EOF {
				return fmt.Errorf("初始化输出结束时遇到 EOF")
			}
			return err
		}

		lineStr := string(line)
		logger.Debug("跳过初始化输出: %s", lineStr)

		// 跳过初始化输出
		if strings.HasPrefix(lineStr, "2025-03-07T") && (strings.Contains(lineStr, "INF") || strings.Contains(lineStr, "WRN")) {
			continue
		}

		// 检查是否是数据包开始
		if utils.PacketStart.Match(line) {
			// 回退读取位置以便后续处理
			r.Discard(-len(line))
			logger.Debug("找到数据包开始: %s", lineStr)
			return nil
		}

		if skipCount++; skipCount > maxHeaderLines {
			return fmt.Errorf("超过最大头部跳行数 %d", maxHeaderLines)
		}
	}
}
