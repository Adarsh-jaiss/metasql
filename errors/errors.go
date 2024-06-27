package errors

import "errors"

var (
	ErrDSNEmpty     = errors.New("dsn is empty")
	ErrRedshiftDSNInvalid   = errors.New("dsn is invalid: workgroup(name)/database or username@cluster(name)/database or secrets_arn")
	ErrNotSupported = errors.New("not supported")
	ErrInTx         = errors.New("query in transaction")
	ErrNotInTx      = errors.New("not in transaction")
	
)