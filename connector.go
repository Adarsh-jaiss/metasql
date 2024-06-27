package metasql

import (
	"context"
	"database/sql/driver"

	"github.com/adarsh-jaiss/metasql/config"
)

type redshiftDataConnector struct {
	d   *redshiftDataDriver
	cfg *config.RedshiftDataConfig
}

func (c *redshiftDataConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := NewRedshiftDataClient(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return NewConnection(client, c.cfg), nil
}

func (c *redshiftDataConnector) Driver() driver.Driver {
	return c.d
}
