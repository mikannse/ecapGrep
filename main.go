package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"home/mikannse/netproc/ecapGrpc/internal/capture"
	internalgreptime "home/mikannse/netproc/ecapGrpc/internal/greptime"
	"home/mikannse/netproc/ecapGrpc/internal/logger"
	"home/mikannse/netproc/ecapGrpc/internal/packet"
	"home/mikannse/netproc/ecapGrpc/internal/utils"
)

const (
	batchSize    = 100
	writeTimeout = 5 * time.Second
)

func main() {
	// 初始化数据库连接
	cli, tbl, err := internalgreptime.SetupGreptimeTable("127.0.0.1", "public")
	if err != nil {
		logger.Error("数据库连接失败: %v", err)
		return
	}

	// 启动捕获命令
	password, err := utils.ReadPassword()
	if err != nil {
		logger.Error("密码错误: %v", err)
		return
	}

	cmd, stdout, err := capture.RunCapture(password)
	if err != nil {
		logger.Error("启动捕获失败: %v", err)
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
		logger.Debug("收到终止信号，正在清理...")
		cancel()

		// 终止 ecapture 进程
		if err := capture.TerminateEcapture(cmd, password); err != nil {
			logger.Error("终止 ecapture 进程失败: %v", err)
		}
	}()

	// 主处理流程
	reader := utils.NewBufferedReader(stdout)
	packetBuffer := make([]*packet.Packet, 0, batchSize)

	// 跳过初始输出
	if err := utils.SkipInitialOutput(&utils.BufferedReaderAdapter{reader}); err != nil {
		logger.Error("跳过初始化信息失败: %v", err)
		return
	}

	logger.Debug("开始主处理流程...")
	for {
		select {
		case <-ctx.Done():
			logger.Debug("上下文取消，退出主循环")
			goto FINAL_FLUSH
		default:
			packet, err := packet.ParsePacket(reader)
			if err != nil {
				if err == utils.EOF {
					logger.Debug("遇到 EOF，等待1秒后重试")
					time.Sleep(1 * time.Second)
					continue
				}
				logger.Error("解析数据包失败: %v", err)
				break
			}

			if packet.Timestamp.IsZero() {
				logger.Debug("跳过无效数据包")
				continue
			}

			// 存入缓冲区
			packetBuffer = append(packetBuffer, packet)

			// 批量写入
			if len(packetBuffer) >= batchSize {
				logger.Debug("批量写入 %d 条数据...", len(packetBuffer))
				if err := internalgreptime.BatchInsert(cli, tbl, packetBuffer); err != nil {
					logger.Error("批量写入错误: %v", err)
				}
				packetBuffer = packetBuffer[:0]
			}
		}
	}

FINAL_FLUSH:
	if len(packetBuffer) > 0 {
		logger.Debug("正在写入剩余 %d 条数据", len(packetBuffer))
		if err := internalgreptime.BatchInsert(cli, tbl, packetBuffer); err != nil {
			logger.Error("最终写入失败: %v", err)
		}
	}
	logger.Debug("程序正常退出")
}
