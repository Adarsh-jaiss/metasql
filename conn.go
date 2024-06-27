package metasql

import (
	"context"
	"database/sql/driver"
	"fmt"

	cfg "github.com/adarsh-jaiss/metasql/config"
	"github.com/adarsh-jaiss/metasql/errors"
)

type redshiftDataConn struct {
	client   RedshiftDataClient
	cfg      *cfg.RedshiftDataConfig
	aliveCh  chan struct{}
	isClosed bool

	inTx          bool
	txOpts        driver.TxOptions
	sqls          []string
	delayedResult []*redshiftDataDelayedResult
}

func NewConnection(client RedshiftDataClient, cfg *cfg.RedshiftDataConfig) *redshiftDataConn {
	return &redshiftDataConn{
		client:  client,
		cfg:     cfg,
		aliveCh: make(chan struct{}),
	}
}

func (conn *redshiftDataConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statment %w", errors.ErrNotSupported)
}
