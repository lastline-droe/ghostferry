package sqlwrapper

import (
	"context"
	sqlorig "database/sql"
)

const maintenanceComment = "/*maintenance:ghostferry*/ "

type DB struct {
	*sqlorig.DB
}

type Tx struct {
	*sqlorig.Tx
}

func Open(driverName, dataSourceName string) (*DB, error) {
	sqlDB, err := sqlorig.Open(driverName, dataSourceName)
	return &DB{sqlDB}, err
}

func (db DB) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return db.DB.PrepareContext(ctx, Comment(query))
}

func (db DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.ExecContext(ctx, Comment(query), args...)
}

func (db DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlorig.Rows, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

func (db DB) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return db.DB.Exec(Comment(query), args...)
}

func (db DB) Prepare(query string) (*sqlorig.Stmt, error) {
	return db.DB.Prepare(Comment(query))
}

func (db DB) Query(query string, args ...interface{}) (*sqlorig.Rows, error) {
	return db.DB.Query(query, args...)
}

func (db DB) QueryRow(query string, args ...interface{}) *sqlorig.Row {
	return db.DB.QueryRow(query, args...)
}

func (db DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sqlorig.Row {
	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	return &Tx{tx}, err
}

func (tx Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.ExecContext(ctx, Comment(query), args...)
}

func (tx Tx) Exec(query string, args ...interface{}) (sqlorig.Result, error) {
	return tx.Tx.Exec(Comment(query), args...)
}

func (tx Tx) Prepare(query string) (*sqlorig.Stmt, error) {
	return tx.Tx.Prepare(Comment(query))
}

func (tx Tx) PrepareContext(ctx context.Context, query string) (*sqlorig.Stmt, error) {
	return tx.Tx.PrepareContext(ctx, Comment(query))
}

func (tx Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sqlorig.Rows, error) {
	return tx.Tx.QueryContext(ctx, query, args)
}

func (tx Tx) Query(query string, args ...interface{}) (*sqlorig.Rows, error) {
	return tx.Tx.Query(query, args...)
}

func (tx Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sqlorig.Row {
	return tx.Tx.QueryRowContext(ctx, query, args...)
}

func (tx Tx) QueryRow(query string, args ...interface{}) *sqlorig.Row {
	return tx.Tx.QueryRow(query, args...)
}

/* SQL query comments are used to differentiate Technical Ghostferry's binlog events from Business binlog events */
func Comment(query string) string {
	return maintenanceComment + query
}
