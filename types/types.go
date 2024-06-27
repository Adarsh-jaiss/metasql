package types

type RedshiftDataTx struct {
	OnCommit   func() error
	OnRollback func() error
}

func (tx *RedshiftDataTx) Commit() error {
	// debugLogger.Printf("tx commit called")
	return tx.OnCommit()
}

func (tx *RedshiftDataTx) Rollback() error {
	// debugLogger.Printf("tx rollback called")
	return tx.OnRollback()
}
