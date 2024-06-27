package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/adarsh-jaiss/metasql/errors"
	"github.com/adarsh-jaiss/metasql/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

// RedshiftDataConfig is a struct that contains the configuration the configuration needed to connect to a Redshift database using the Redshift Data API.
// It includes the ClusterIdentifier, Database, DBUser, WorkgroupName, SecretArn, Timeout, Polling, Params, and RedshiftDataOptFns fields
type RedshiftDataConfig struct {
	ClusterIdentifier  *string                       `yaml:"" pflag:",cluster-identifier"`                   // ClusterIdentifier is the name of the Redshift cluster
	Database           *string                       `yaml:"" pflag:",database"`                             // Database is the name of the database
	DBUser             *string                       `yaml:"" pflag:",db-user"`                              // DBUser is the username for the database
	WorkgroupName      *string                       `yaml:"" pflag:",workgroup-name"`                       // WorkgroupName is the name of the workgroup
	SecretsArn         *string                       `yaml:"" pflag:",secret-arn"`                           // SecretArn is the ARN of the secret
	Timeout            time.Duration                 `yaml:"timeout" pflag:",timeout"`                       // Timeout is the amount of time to wait for the query to complete
	Polling            time.Duration                 `yaml:"polling" pflag:",polling"`                       // Polling is the amount of time to wait between polling for the query status
	Params             url.Values                    `yaml:"params" pflag:",params"`                         // Params is a map of key value pairs to be used as parameters in the query
	RedshiftDataOptFns []func(*redshiftdata.Options) `yaml:"redshiftdataoptfns" pflag:",redshiftdataoptfns"` // RedshiftDataOptFns is a slice of functions that modify the RedshiftDataClient options
	// Region             *string                        `yaml:"region" pflag:",region"`                         // Region is the AWS region
}

// addOrDeleteParam adds or deletes a parameter based on its value.
func AddOrDeleteParam(params url.Values, key string, value fmt.Stringer) {
	if value.String() != "0" { // Assuming String() returns "0" for zero values
		params.Add(key, value.String())
	} else {
		params.Del(key)
	}
}

// String() Returns a string representation of the RedshiftDataConfig, suitable for logging or debugging.
// It includes the connection details and any specified parameters.
func (cfg *RedshiftDataConfig) String() string {
	base := strings.TrimPrefix(cfg.BaseString(), "//")
	if base == "" {
		return ""
	}
	params := url.Values{}
	AddOrDeleteParam(params, "timeout", cfg.Timeout)
	AddOrDeleteParam(params, "polling", cfg.Polling)

	EncodedParams := params.Encode()
	if EncodedParams != "" {
		return base + "?" + EncodedParams
	}
	return base
}

// BaseString Generates the base connection string based on the configuration.
// It supports Secrets Manager ARN, cluster identifier with database user, and workgroup name.
func (cfg *RedshiftDataConfig) BaseString() string {
	if cfg.SecretsArn != nil {
		return *cfg.SecretsArn
	}

	var u url.URL
	if cfg.ClusterIdentifier != nil && cfg.DBUser != nil {
		u.Host = fmt.Sprintf("cluster(%s)", *cfg.ClusterIdentifier)
		u.User = url.User(*cfg.DBUser)
	}

	if cfg.WorkgroupName != nil {
		u.Host = *cfg.WorkgroupName
	}

	if cfg.Database != nil {
		u.Path = *cfg.Database
	}

	if u.Host == "" || cfg.Database == nil {
		return ""
	}
	return u.String()
}

// Set Params, Parses and sets configuration parameters from a url.Values object.
// It Handles special parameters like timeout and polling, converting them into appropriate types.
func (cfg *RedshiftDataConfig) SetParams(params url.Values) error {
	var err error
	cfg.Params = params
	if params.Has("timeout") {
		cfg.Timeout, err = time.ParseDuration(params.Get("timeout"))
		if err != nil {
			return fmt.Errorf("error parsing timeout: %w", err)
		}
		cfg.Params.Del("timeout")
	}
	if params.Has("polling") {
		cfg.Polling, err = time.ParseDuration(params.Get("polling"))
		if err != nil {
			return fmt.Errorf("error parsing polling: %w", err)
		}
		cfg.Params.Del("polling")
	}

	if params.Has("region") {
		cfg = cfg.WithRegion(params.Get("region"))
	}
	if len(params) == 0 {
		return nil
	}

	return nil
}

// WithRegion sets the AWS region for the RedshiftData API client and returns the updated configuration object.
// It adds the region to the Params and RedshiftDataOptFns fields
func (cfg *RedshiftDataConfig) WithRegion(region string) *RedshiftDataConfig {
	if cfg.Params == nil {
		cfg.Params = url.Values{}
	}
	cfg.Params.Set("region", region)
	cfg.RedshiftDataOptFns = append(cfg.RedshiftDataOptFns, func(o *redshiftdata.Options) {
		o.Region = region
	})
	return cfg
}

// ParseDSN Parses a Data Source Name (DSN) string into a RedshiftDataConfig object. 
// It Supports ARN-based connections, cluster-based connections, and workgroup-based connections.
func ParseDSN(dsn string) (*RedshiftDataConfig, error) {
	if dsn == "" {
		return nil, errors.ErrDSNEmpty
	}
	if strings.HasPrefix(dsn, "arn:") {
		parts := strings.Split(dsn, "?")
		cfg := &RedshiftDataConfig{
			SecretsArn: aws.String(parts[0]),
		}
		if len(parts) >= 2 {
			params, err := url.ParseQuery(strings.Join(parts[1:], "?"))
			if err != nil {
				return nil, fmt.Errorf("dsn is invalid: can not parse query params: %w", err)
			}
			if err := cfg.SetParams(params); err != nil {
				return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
			}
		}
		return cfg, nil
	}

	u, err := url.Parse("redshift-data://" + dsn)
	if err != nil {
		return nil, fmt.Errorf("dsn is invalid: %w", err)
	}

	cfg := &RedshiftDataConfig{
		Database: utils.Nullif(strings.TrimPrefix(u.Path, "/")),
	}
	if cfg.Database == nil {
		return nil, fmt.Errorf("dsn is invalid: database is missing")
	}

	if err := cfg.SetParams(u.Query()); err != nil {
		return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
	}
	if strings.HasPrefix(u.Host, "cluster(") {
		cfg.DBUser = utils.Nullif(u.User.Username())
		cfg.ClusterIdentifier = utils.Nullif(strings.TrimSuffix(strings.TrimPrefix(u.Host, "cluster("), ")"))
		return cfg, nil
	}
	if strings.HasPrefix(u.Host, "workgroup(") {
		cfg.WorkgroupName = utils.Nullif(strings.TrimSuffix(strings.TrimPrefix(u.Host, "workgroup("), ")"))
		return cfg, nil
	}

	return nil, errors.ErrRedshiftDSNInvalid

}
