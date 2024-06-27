package metasql

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/config"

	cfg "github.com/adarsh-jaiss/metasql/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

// RedshiftDataClient is an interface for the RedshiftDataClient
// It includes the ExecuteStatement, DescribeStatement, CancelStatement, BatchExecuteStatement, and GetStatementResultAPIClient methods
type RedshiftDataClient interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optfunc ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optfunc ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	CancelStatement(ctx context.Context, params *redshiftdata.CancelStatementInput, optfunc ...func(*redshiftdata.Options)) (*redshiftdata.CancelStatementOutput, error)
	BatchExecuteStatement(ctx context.Context, params *redshiftdata.BatchExecuteStatementInput, optfunc ...func(*redshiftdata.Options)) (*redshiftdata.BatchExecuteStatementOutput, error)
	redshiftdata.GetStatementResultAPIClient
}

// RedshiftDataClientConstructor is a function signature for creating a RedshiftDataClient
// The function is expected to return a RedshiftDataClient and an error
var RedshiftDataClientConstructor func(ctx context.Context, cfg *cfg.RedshiftDataConfig) (RedshiftDataClient, error)

// NewRedshiftDataClient creates a new RedshiftDataClient
// It uses the RedshiftDataClientConstructor function if it is not nil
// Otherwise it uses the DefaultRedshiftDataClientConstructor
func NewRedshiftDataClient(ctx context.Context, cfg *cfg.RedshiftDataConfig) (RedshiftDataClient, error) {
	if RedshiftDataClientConstructor != nil {
		return RedshiftDataClientConstructor(ctx, cfg)
	}
	return DefaultRedshiftDataClientConstructor(ctx, cfg)
}

// DefaultRedshiftDataClientConstructor creates a new RedshiftDataClient using the default AWS SDK configuration
// It uses the AWS SDK's LoadDefaultConfig function to load the default configuration
// It then creates a new RedshiftDataClient using the configuration and the RedshiftDataOptFns passed in the cfg
// It returns the RedshiftDataClient and an error
func DefaultRedshiftDataClientConstructor(ctx context.Context, cfg *cfg.RedshiftDataConfig) (RedshiftDataClient, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := redshiftdata.NewFromConfig(awsCfg, cfg.RedshiftDataOptFns...)
	return client, nil
}
