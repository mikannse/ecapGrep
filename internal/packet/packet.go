package packet

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"home/mikannse/netproc/ecapGrpc/internal/logger"
	"home/mikannse/netproc/ecapGrpc/internal/utils"
)

const (
	packetStartRegex = `^UUID:\d+_\d+_\w+_\d+_\d+_(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d+):\d+`
)

type Packet struct {
	Timestamp time.Time
	SourceIP  string
	Content   string
}

var (
	packetPool = utils.NewSyncPool(func() interface{} {
		return &Packet{}
	})
	timestampRe = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2}))`)
	sourceIPRe  = regexp.MustCompile(packetStartRegex)
	packetStart = regexp.MustCompile(packetStartRegex)
)

func ParsePacket(reader *bufio.Reader) (*Packet, error) {
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
		logger.Debug("检查数据包起始行: %s", lineStr)

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
