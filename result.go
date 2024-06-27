package metasql

import (
	"database/sql/driver"

	"github.com/adarsh-jaiss/metasql/types"
)

type redshiftDataDelayedResult struct {
	driver.Result
}


func NewResultWithSubStatementData(st types.SubStatementData) *redshiftDataResult {
	// debugLogger.Printf("[%s] create result", coalesce(st.Id))
	return &redshiftDataResult{
		affectedRows: st.ResultRows,
	}
}