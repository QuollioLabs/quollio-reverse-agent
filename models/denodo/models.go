package denodo

import "database/sql"

type GetDatabasesResult struct {
	DatabaseName string         `db:"db_name"`
	Description  sql.NullString `db:"description"`
}

type GetViewsResult struct {
	DatabaseName string         `db:"database_name"`
	ViewName     string         `db:"name"`
	ViewType     int            `db:"view_type"`
	Description  sql.NullString `db:"description"`
}

type GetViewColumnsResult struct {
	DatabaseName  string         `db:"database_name"`
	ViewType      int            `db:"view_type"`
	ViewName      string         `db:"view_name"`
	ColumnName    string         `db:"column_name"`
	ColumnRemarks sql.NullString `db:"column_remarks"`
}
