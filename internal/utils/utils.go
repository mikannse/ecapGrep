package utils

import (
	"bufio"
	"fmt"
	"home/mikannse/netproc/ecapGrpc/internal/logger"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/term"
)

const (
	maxHeaderLines = 200
)

var (
	PacketStart = regexp.MustCompile(`^UUID:\d+_\d+_\w+_\d+_\d+_(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d+):\d+`)
	EOF         = io.EOF
)

func NewBufferedReader(r io.Reader) *bufio.Reader {
	return bufio.NewReader(r)
}

func NewScanner(r io.Reader) *bufio.Scanner {
	return bufio.NewScanner(r)
}

func NewSyncPool(newFunc func() interface{}) *sync.Pool {
	return &sync.Pool{
		New: newFunc,
	}
}

func ReadPassword() (string, error) {
	fmt.Print("请输入 sudo 密码: ")
	pass, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", fmt.Errorf("密码读取失败: %w", err)
	}
	fmt.Println()
	return string(pass), nil
}

func SkipInitialOutput(r BufferedReader) error {
	skipCount := 0
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == EOF {
				return fmt.Errorf("初始化输出结束时遇到 EOF")
			}
			return err
		}

		lineStr := string(line)
		Debug("跳过初始化输出: %s", lineStr)

		// 跳过初始化输出
		if strings.HasPrefix(lineStr, "2025-03-07T") && (strings.Contains(lineStr, "INF") || strings.Contains(lineStr, "WRN")) {
			continue
		}

		// 检查是否是数据包开始
		if PacketStart.Match(line) {
			// 回退读取位置以便后续处理
			r.Discard(-len(line))
			Debug("找到数据包开始: %s", lineStr)
			return nil
		}

		if skipCount++; skipCount > maxHeaderLines {
			return fmt.Errorf("超过最大头部跳行数 %d", maxHeaderLines)
		}
	}
}

type BufferedReader interface {
	ReadLine() ([]byte, bool, error)
	Discard(int) int
}

// BufferedReaderAdapter 适配器模式，将 bufio.Reader 转换为 BufferedReader
type BufferedReaderAdapter struct {
	*bufio.Reader
}

func (br *BufferedReaderAdapter) Discard(n int) int {
	n, _ = br.Reader.Discard(n)
	return n
}

func Debug(format string, args ...interface{}) {
	logger.Debug(format, args...)
}

func Error(format string, args ...interface{}) {
	logger.Error(format, args...)
}
