package metasql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	cfg "github.com/adarsh-jaiss/metasql/config"
	"github.com/adarsh-jaiss/metasql/errors"
	"github.com/adarsh-jaiss/metasql/types"
	"github.com/adarsh-jaiss/metasql/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	awstypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// A redshiftDataConn struct represents a connection to an AWS Redshift database using the Redshift Data API.
// It encapsulates the client used to communicate with the Redshift Data API, configuration details, transaction state, and other operational flags.
type redshiftDataConn struct {
	client   RedshiftDataClient      // RedshiftDataClient is an interface that defines the methods required to communicate with the Redshift Data API.
	cfg      *cfg.RedshiftDataConfig // RedshiftDataConfig is a struct that holds the configuration details required to connect to an AWS Redshift database using the Redshift Data API.
	aliveCh  chan struct{}           // aliveCh is a channel that is closed when the connection is closed.
	isClosed bool                    // isClosed is a flag that indicates whether the connection is closed.

	inTx          bool                         // inTx is a flag that indicates whether the connection is in a transaction.
	txOpts        driver.TxOptions             // txOpts is a struct that holds the transaction options.
	sqls          []string                     // sqls is a slice that holds the SQL statements executed in the transaction.
	delayedResult []*redshiftDataDelayedResult // delayedResult is a slice that holds the delayed results of the SQL statements executed in the transaction.
}

// NewConnection returns a new redshiftDataConn instance with the provided RedshiftDataClient and RedshiftDataConfig.
func NewConnection(client RedshiftDataClient, cfg *cfg.RedshiftDataConfig) *redshiftDataConn {
	return &redshiftDataConn{
		client:  client,
		cfg:     cfg,
		aliveCh: make(chan struct{}),
	}
}

// Prepares a SQL statement for execution.
// This method returns an error indicating that prepared statements are not supported by this driver.
func (conn *redshiftDataConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statment %w", errors.ErrNotSupported)
}

// Prepare  A convenience wrapper around PrepareContext, using context.Background() as the context.
// It also indicates that prepared statements are not supported.
func (conn *redshiftDataConn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

// Close : Closes the connection.
// If the connection is already marked as closed (isClosed is true), it does nothing. Otherwise, it marks the connection as closed and closes the aliveCh channel to signal that the connection is no longer alive.
func (conn *redshiftDataConn) Close() error {
	if conn.isClosed {
		return nil
	}

	conn.isClosed = true
	close(conn.aliveCh)
	return nil
}

func (conn *redshiftDataConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if conn.inTx {
		return nil, errors.ErrInTx
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("isolation level %w", errors.ErrNotSupported)
	}

	conn.inTx = true
	conn.txOpts = opts
	cleanup := func() error {
		conn.inTx = false
		conn.txOpts = driver.TxOptions{}
		conn.sqls = nil
		conn.delayedResult = nil
		return nil
	}
	tx := &types.RedshiftDataTx{
		OnRollback: func() error {
			if !conn.inTx {
				return errors.ErrNotInTx
			}
			err := cleanup()
			if err != nil {
				return fmt.Errorf("rollback error : %w", err)
			}
			return nil
		},

		OnCommit: func() error {
			if !conn.inTx {
				return errors.ErrNotInTx
			}
			if len(conn.sqls) == 0 {
				return cleanup()
			}
			if len(conn.sqls) != len(conn.delayedResult) {
				panic(fmt.Sprintf("unexpected length of sqls and delayedResult: %d != %d", len(conn.sqls), len(conn.delayedResult)))
			}
			if len(conn.sqls) == 1 {
				res, err := conn.ExecContext(ctx, conn.sqls[0], []driver.NamedValue{})
				if err != nil {
					return fmt.Errorf("commit error: %w", err)
				}
				if conn.delayedResult[0] != nil {
					conn.delayedResult[0].Result = res
				}
				return nil
			}

			input := &redshiftdata.BatchExecuteStatementInput{
				Sqls: append(make([]string, 0, len(conn.sqls)), conn.sqls...),
			}
			_, desc, err := conn.BatchExecuteStatement(ctx, input)
			if err != nil {
				return err
			}
			for i := range input.Sqls {
				if i >= len(desc.SubStatements) {
					return fmt.Errorf("sub statement not found: %d", i)
				}
				if conn.delayedResult[i] != nil {
					conn.delayedResult[i].Result = NewResultWithSubStatementData(desc.SubStatements[i])
				}
			}
			return cleanup()
		},
	}

	return tx, nil
}

func (conn *redshiftDataConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *redshiftDataConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if conn.inTx {
		return nil, errors.ErrInTx
	}

	params := &redshiftdata.ExecuteStatementInput{
		Sql:      utils.Nullif(rewriteQuery(query, len(args))),
		Parameters: convertArgsToParameters(args),
	}

	p,output,err := conn.ExecuteStatement(ctx, params)
	if err != nil {
		return nil, err
	}
	rows := newRows(utils.Coalesce(output.ID),p)
	return rows,nil

}

func (conn *redshiftDataConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if conn.inTx {
		if len(args) > 0 {
			return nil, fmt.Errorf("exec with args in transaction: %w", errors.ErrNotSupported)
		}
		if conn.txOpts.ReadOnly {
			return nil, fmt.Errorf("exec in read only transaction: %w", errors.ErrNotSupported)
		}
		conn.sqls = append(conn.sqls, query)
		result := &redshiftDataDelayedResult{}
		conn.delayedResult = append(conn.delayedResult, result)
		// debugLogger.Printf("delayedResult[%d] creaed for %q", len(conn.delayedResult)-1, query)
		return &redshiftDataDelayedResult{}, nil
	}

	params := &redshiftdata.ExecuteStatementInput{
		Sql:        utils.Nullif(rewriteQuery(query, len(args))),
		Parameters: convertArgsToParameters(args),
	}

	_, output, err := conn.executeStatement(ctx, params)
	if err != nil {
		return nil, err
	}
	return newResult(output),nil
	
}

func rewriteQuery(query string, paramsCount int) string {
	if paramsCount == 0 {
		return query
	}
	runes := make([]rune, 0, len(query))
	stack := make([]rune, 0)
	var exclamationCount int
	for _, r := range query {
		if len(stack) > 0 {
			if r == stack[len(stack)-1] {
				stack = stack[:len(stack)-1]
				runes = append(runes, r)
				continue
			}
		} else {
			switch r {
			case '?':
				exclamationCount++
				runes = append(runes, []rune(fmt.Sprintf(":%d", exclamationCount))...)
				continue
			case '$':
				runes = append(runes, ':')
				continue
			}
		}
		switch r {
		case '"', '\'':
			stack = append(stack, r)
		}
		runes = append(runes, r)
	}
	return string(runes)
}

func convertArgsToParameters(args []driver.NamedValue) []awstypes.SqlParameter {
	if len(args) == 0 {
		return nil
	}
	params := make([]awstypes.SqlParameter, 0, len(args))
	for _, arg := range args {
		params = append(params, awstypes.SqlParameter{
			Name:  aws.String(utils.Coalesce(utils.Nullif(arg.Name), aws.String(fmt.Sprintf("%d", arg.Ordinal)))),
			Value: aws.String(fmt.Sprintf("%v", arg.Value)),
		})
	}
	return params
}

func(conn *redshiftDataConn) executeStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput) (*redshiftdata.GetStatementResultPaginator, *redshiftdata.DescribeStatementOutput, error) {
	// debugLogger.Printf("query: %s", utils.Coalesce(params.Sql))
	params.ClusterIdentifier = conn.cfg.ClusterIdentifier
	params.Database = conn.cfg.Database
	params.DbUser = conn.cfg.DBUser
	params.SecretArn = conn.cfg.SecretsArn
	params.WorkgroupName = conn.cfg.WorkgroupName

	executeOutput, err := conn.client.ExecuteStatement(ctx, params)
	if err != nil {
		return nil, nil, fmt.Errorf("execute statement error: %w", err)
	}
	queryStartTime := time.Now()
	// debugLogger.Printf("[%s] success execute statement: %s", *executeOutput.Id, utils.Coalesce(params.Sql))
	describeOutput, err := conn.waitWithCancel(ctx, executeOutput.Id, queryStartTime)
	if err != nil {
		return nil, nil, err
	}
	if describeOutput.Status == awstypes.StatusStringAborted {
		return nil, nil, fmt.Errorf("query aborted: %s", *describeOutput.Error)
	}
	if describeOutput.Status == awstypes.StatusStringFailed {
		return nil, nil, fmt.Errorf("query failed: %s", *describeOutput.Error)
	}
	if describeOutput.Status != awstypes.StatusStringFinished {
		return nil, nil, fmt.Errorf("query status is not finished: %s", describeOutput.Status)
	}
	// debugLogger.Printf("[%s] success query: elapsed_time=%s", *executeOutput.Id, time.Since(queryStartTime))
	if !*describeOutput.HasResultSet {
		return nil, describeOutput, nil
	}
	// debugLogger.Printf("[%s] query has result set: result_rows=%d", *executeOutput.Id, describeOutput.ResultRows)
	p := redshiftdata.NewGetStatementResultPaginator(conn.client, &redshiftdata.GetStatementResultInput{
		Id: executeOutput.Id,
	})
	return p, describeOutput, nil
}



func (conn *redshiftDataConn) BatchExecuteStatement(ctx context.Context, input *redshiftdata.BatchExecuteStatementInput) (*redshiftdata.BatchExecuteStatementOutput, *redshiftdata.DescribeStatementOutput, error) {

}
