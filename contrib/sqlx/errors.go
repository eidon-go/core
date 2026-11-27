package sqlx

import "errors"

var (
	// ErrUnknownDriver is returned by NewManager for a DB whose DriverName
	// sqlx has no bind type for (an empty name from sql.NewFromDB included):
	// pass sql.WithDriverName, or register the name with sqlx.BindDriver.
	ErrUnknownDriver = errors.New("sqlx: driver with unknown bind type")

	// ErrNilDB is returned by NewManager for a nil *sql.DB.
	ErrNilDB = errors.New("sqlx: nil *sql.DB")
)
