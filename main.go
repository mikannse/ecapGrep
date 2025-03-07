package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	batchSize      = 100
	writeTimeout   = 5 * time.Second
	timestampRegex = `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$`
	sourceIPRegex  = `UUID:\d+_\d+_\w+_\d+_\d+_(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d+):\d+`
)

type Packet struct {
	Timestamp time.Time
	SourceIP  string
	Content   string
}

var (
	timestampRe = regexp.MustCompile(timestampRegex)
	sourceIPRe  = regexp.MustCompile(sourceIPRegex)
)

func main() {
	// 初始化 GreptimeDB 客户端
	cfg := greptime.NewConfig("127.0.0.1").WithDatabase("public")
	cli, err := greptime.NewClient(cfg)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}

	// 创建表
	tbl, err := setupGreptimeTable()
	if err != nil {
		log.Fatalf("表创建失败: %v", err)
	}

	// 启动捕获命令（假设使用 ecapture）
	cmd := exec.Command("sudo", "./ecapture", "tls")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("标准输出管道错误: %v", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("启动命令失败: %v", err)
	}
	defer cmd.Wait()

	// 主处理流程
	reader := bufio.NewReader(stdout)
	packetBuffer := make([]*Packet, 0, batchSize)

	for {
		packet, err := parsePacket(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("解析数据包失败: %v", err)
			continue
		}

		packetBuffer = append(packetBuffer, packet)
		if len(packetBuffer) >= batchSize {
			if err := batchInsert(cli, tbl, packetBuffer); err != nil {
				log.Printf("批量写入失败: %v", err)
			}
			packetBuffer = packetBuffer[:0]
		}
	}

	// 写入剩余数据
	if len(packetBuffer) > 0 {
		if err := batchInsert(cli, tbl, packetBuffer); err != nil {
			log.Printf("最终写入失败: %v", err)
		}
	}
}

// 设置 GreptimeDB 表结构
func setupGreptimeTable() (*table.Table, error) {
	tbl, err := table.New("packet_data")
	if err != nil {
		return nil, fmt.Errorf("创建表失败: %w", err)
	}

	if err := tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND); err != nil {
		return nil, fmt.Errorf("添加时间戳列失败: %w", err)
	}
	if err := tbl.AddFieldColumn("source_ip", types.STRING); err != nil {
		return nil, fmt.Errorf("添加 source_ip 列失败: %w", err)
	}
	if err := tbl.AddFieldColumn("packet_content", types.STRING); err != nil {
		return nil, fmt.Errorf("添加 packet_content 列失败: %w", err)
	}
	return tbl, nil
}

// 解析数据包
func parsePacket(reader *bufio.Reader) (*Packet, error) {
	var (
		timestamp time.Time
		sourceIP  string
		content   strings.Builder
	)

	// 读取时间戳行
	tsLine, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	tsLineStr := strings.TrimSpace(string(tsLine))
	if !timestampRe.MatchString(tsLineStr) {
		return nil, fmt.Errorf("无效的时间戳行: %s", tsLineStr)
	}
	timestamp, err = time.Parse("2006-01-02T15:04:05-07:00", tsLineStr)
	if err != nil {
		return nil, fmt.Errorf("时间戳解析失败: %w", err)
	}

	// 读取头部行
	headerLine, _, err := reader.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("读取头部行失败: %w", err)
	}
	headerLineStr := strings.TrimSpace(string(headerLine))

	// 解析源 IP
	ipMatch := sourceIPRe.FindStringSubmatch(headerLineStr)
	if len(ipMatch) < 2 {
		sourceIP = "0.0.0.0"
	} else {
		sourceIP = ipMatch[1]
	}

	// 将头部行加入内容
	content.WriteString(headerLineStr + "\n")

	// 读取后续内容
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("读取内容失败: %w", err)
		}
		lineStr := strings.TrimSpace(string(line))

		// 遇到空行或新时间戳行，结束当前数据包
		if lineStr == "" || timestampRe.MatchString(lineStr) {
			if timestampRe.MatchString(lineStr) {
				reader.Discard(-len(line))
			}
			break
		}
		content.WriteString(lineStr + "\n")
	}

	return &Packet{
		Timestamp: timestamp,
		SourceIP:  sourceIP,
		Content:   strings.TrimSpace(content.String()),
	}, nil
}

// 批量写入数据库
func batchInsert(cli *greptime.Client, tbl *table.Table, packets []*Packet) error {
	for _, p := range packets {
		if err := tbl.AddRow(p.SourceIP, p.Timestamp, p.Content); err != nil {
			return fmt.Errorf("添加行失败: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if _, err := cli.Write(ctx, tbl); err != nil {
		return fmt.Errorf("数据库写入失败: %w", err)
	}
	return nil
}
