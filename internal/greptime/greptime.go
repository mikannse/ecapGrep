package greptime

import (
	"context"
	"fmt"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"

	"home/mikannse/netproc/ecapGrpc/internal/logger"
	"home/mikannse/netproc/ecapGrpc/internal/packet"
)

const (
	writeTimeout = 5 * time.Second
)

func SetupGreptimeTable(host, database string) (*greptime.Client, *table.Table, error) {
	cfg := greptime.NewConfig(host).WithDatabase(database)
	cli, err := greptime.NewClient(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("创建客户端失败: %w", err)
	}

	tbl, err := table.New("packet_headers")
	if err != nil {
		return nil, nil, fmt.Errorf("创建表失败: %w", err)
	}

	columns := []struct {
		name string
		typ  types.ColumnType
	}{
		{"source_ip", types.STRING},
		{"ts", types.TIMESTAMP_MILLISECOND},
		{"content", types.STRING},
	}

	for _, col := range columns {
		if col.name == "ts" {
			if err := tbl.AddTimestampColumn(col.name, col.typ); err != nil {
				return nil, nil, fmt.Errorf("添加时间戳列失败: %w", err)
			}
		} else {
			if err := tbl.AddFieldColumn(col.name, col.typ); err != nil {
				return nil, nil, fmt.Errorf("添加 %s 列失败: %w", col.name, err)
			}
		}
	}
	return cli, tbl, nil
}

func BatchInsert(cli *greptime.Client, tbl *table.Table, packets []*packet.Packet) error {
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

	logger.Debug("成功写入 %d 条记录", len(packets))
	return nil
}
