package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"golang.org/x/term"
)

const (
	batchSize        = 100
	writeTimeout     = 5 * time.Second
	maxHeaderLines   = 200 // 增加最大头部跳行数
	packetStartRegex = `^UUID:\d+_\d+_\w+_\d+_\d+_(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d+):\d+`
)

type Packet struct {
	Timestamp time.Time
	SourceIP  string
	Content   string // 添加这一字段
}

type logger struct {
	debug bool
}

var (
	packetPool = sync.Pool{
		New: func() interface{} {
			return &Packet{}
		},
	}
	loggerInstance = &logger{debug: true}

	timestampRe = regexp.MustCompile(
		`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2}))`,
	)
	sourceIPRe  = regexp.MustCompile(packetStartRegex)
	packetStart = regexp.MustCompile(packetStartRegex)
)

func (l *logger) Debug(format string, args ...interface{}) {
	if l.debug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func (l *logger) Error(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func setupGreptimeTable() (*table.Table, error) {
	tbl, err := table.New("packet_headers")
	if err != nil {
		return nil, fmt.Errorf("创建表失败: %w", err)
	}

	columns := []struct {
		name string
		typ  types.ColumnType
	}{
		{"source_ip", types.STRING},
		{"ts", types.TIMESTAMP_MILLISECOND},
		{"content", types.STRING}, // 添加这一列
	}

	for _, col := range columns {
		if col.name == "ts" {
			if err := tbl.AddTimestampColumn(col.name, col.typ); err != nil {
				return nil, fmt.Errorf("添加时间戳列失败: %w", err)
			}
		} else {
			if err := tbl.AddFieldColumn(col.name, col.typ); err != nil {
				return nil, fmt.Errorf("添加 %s 列失败: %w", col.name, err)
			}
		}
	}
	return tbl, nil
}

func skipInitialOutput(r *bufio.Reader) error {
	skipCount := 0
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("初始化输出结束时遇到 EOF")
			}
			return err
		}

		lineStr := string(line)
		loggerInstance.Debug("跳过初始化输出: %s", lineStr)

		// 跳过初始化输出
		if strings.HasPrefix(lineStr, "2025-03-07T") && (strings.Contains(lineStr, "INF") || strings.Contains(lineStr, "WRN")) {
			continue
		}

		// 检查是否是数据包开始
		if packetStart.Match(line) {
			// 回退读取位置以便后续处理
			r.Discard(-len(line))
			loggerInstance.Debug("找到数据包开始: %s", lineStr)
			return nil
		}

		if skipCount++; skipCount > maxHeaderLines {
			return fmt.Errorf("超过最大头部跳行数 %d", maxHeaderLines)
		}
	}
}

func parsePacket(reader *bufio.Reader) (*Packet, error) {
	var (
		headerLine string
		content    strings.Builder
		foundStart bool
	)

	// 读取直到找到有效起始行
	for !foundStart {
		line, _, err := reader.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("读取起始行失败: %w", err)
		}

		lineStr := string(line)
		loggerInstance.Debug("检查数据包起始行: %s", lineStr)

		if packetStart.MatchString(lineStr) {
			headerLine = lineStr
			foundStart = true
		}
	}

	// 解析头部信息
	ts, ip, err := parseHeaderLine(headerLine)
	if err != nil {
		return nil, fmt.Errorf("头部解析失败: %w", err)
	}

	// 读取后续内容直到下一个数据包开始
	for {
		// 预读下一个字节
		nextByte, err := reader.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("预读失败: %w", err)
		}

		// 检查是否开始新数据包
		if nextByte[0] == 'U' { // UUID 开头标识
			if isNewPacketStart(reader) {
				break
			}
		}

		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("内容读取失败: %w", err)
		}

		content.Write(line)
		content.WriteByte('\n')
	}

	return &Packet{
		Timestamp: ts,
		SourceIP:  ip,
		Content:   strings.TrimSpace(content.String()),
	}, nil
}

func isNewPacketStart(reader *bufio.Reader) bool {
	// 保存当前读取位置
	savePoint, _ := reader.Peek(0)

	// 尝试读取足够长的字节来验证
	testBytes, err := reader.Peek(len("UUID:0000_0000"))
	if err != nil {
		return false
	}

	// 验证是否符合数据包起始模式
	if packetStart.Match(testBytes) {
		return true
	}

	// 恢复读取位置
	reader.Reset(bytes.NewReader(savePoint))
	return false
}

// 改进头部解析
func parseHeaderLine(line string) (time.Time, string, error) {
	// 时间戳解析
	tsMatch := timestampRe.FindStringSubmatch(line)
	if len(tsMatch) < 2 {
		return time.Time{}, "", fmt.Errorf("时间戳格式不匹配: %s", line)
	}

	ts, err := time.Parse(time.RFC3339Nano, tsMatch[1])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("时间解析错误: %w", err)
	}

	// 源IP解析
	ipMatch := sourceIPRe.FindStringSubmatch(line)
	if len(ipMatch) < 2 {
		return ts, "0.0.0.0", nil
	}

	if ip := net.ParseIP(ipMatch[1]); ip != nil {
		return ts, ip.String(), nil
	}
	return ts, "0.0.0.0", nil
}

func runCapture(password string) (*exec.Cmd, io.ReadCloser, error) {
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
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			loggerInstance.Error("[ECAPTURE] %s", scanner.Text())
		}
	}()

	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("启动命令失败: %w", err)
	}

	return cmd, stdout, nil
}

func batchInsert(cli *greptime.Client, tbl *table.Table, packets []*Packet) error {
	for _, p := range packets {
		if err := tbl.AddRow(
			p.SourceIP,  // string
			p.Timestamp, // time.Time
			p.Content,   // string
		); err != nil {
			return fmt.Errorf("行数据添加失败: %w (数据: %+v)", err, p)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if _, err := cli.Write(ctx, tbl); err != nil {
		return fmt.Errorf("数据库写入失败: %w", err)
	}

	loggerInstance.Debug("成功写入 %d 条记录", len(packets))
	return nil
}

func readPassword() (string, error) {
	fmt.Print("请输入 sudo 密码: ")
	pass, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", fmt.Errorf("密码读取失败: %w", err)
	}
	fmt.Println()
	return string(pass), nil
}

func main() {
	// 初始化数据库连接
	cfg := greptime.NewConfig("127.0.0.1").WithDatabase("public")
	cli, err := greptime.NewClient(cfg)
	if err != nil {
		loggerInstance.Error("数据库连接失败: %v", err)
		return
	}

	tbl, err := setupGreptimeTable()
	if err != nil {
		loggerInstance.Error("表创建失败: %v", err)
		return
	}

	// 启动捕获命令
	password, err := readPassword()
	if err != nil {
		loggerInstance.Error("密码错误: %v", err)
		return
	}

	cmd, stdout, err := runCapture(password)
	if err != nil {
		loggerInstance.Error("启动捕获失败: %v", err)
		return
	}
	defer stdout.Close()

	// 信号处理
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		loggerInstance.Debug("收到终止信号，正在清理...")
		cancel()

		// 终止 ecapture 进程
		if err := terminateEcapture(cmd, password); err != nil {
			loggerInstance.Error("终止 ecapture 进程失败: %v", err)
		}
	}()

	// 主处理流程
	reader := bufio.NewReader(stdout)
	packetBuffer := make([]*Packet, 0, batchSize)

	// 跳过初始输出
	if err := skipInitialOutput(reader); err != nil {
		loggerInstance.Error("跳过初始化信息失败: %v", err)
		return
	}

	loggerInstance.Debug("开始主处理流程...")
	for {
		select {
		case <-ctx.Done():
			loggerInstance.Debug("上下文取消，退出主循环")
			goto FINAL_FLUSH
		default:
			packet, err := parsePacket(reader)
			if err != nil {
				if err == io.EOF {
					loggerInstance.Debug("遇到 EOF，等待1秒后重试")
					time.Sleep(1 * time.Second)
					continue
				}
				loggerInstance.Error("解析数据包失败: %v", err)
				break
			}

			if packet.Timestamp.IsZero() {
				loggerInstance.Debug("跳过无效数据包")
				continue
			}

			// 存入缓冲区
			packetBuffer = append(packetBuffer, packet)

			// 批量写入
			if len(packetBuffer) >= batchSize {
				loggerInstance.Debug("批量写入 %d 条数据...", len(packetBuffer))
				if err := batchInsert(cli, tbl, packetBuffer); err != nil {
					loggerInstance.Error("批量写入错误: %v", err)
				}
				packetBuffer = packetBuffer[:0]
			}
		}
	}

FINAL_FLUSH:
	if len(packetBuffer) > 0 {
		loggerInstance.Debug("正在写入剩余 %d 条数据", len(packetBuffer))
		if err := batchInsert(cli, tbl, packetBuffer); err != nil {
			loggerInstance.Error("最终写入失败: %v", err)
		}
	}
	loggerInstance.Debug("程序正常退出")
}

func terminateEcapture(cmd *exec.Cmd, password string) error {
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
