package odbc

import (
	"fmt"
	"quollio-reverse-agent/repository/denodo/odbc/models"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DenodoDBConfig struct {
	Database string
	Host     string
	Port     string
	SslMode  string
}

type Client struct {
	Conn *sqlx.DB
}

func (c *DenodoDBConfig) NewClient(username, password string) (*Client, error) {
	denodoConnStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s sslmode=%s database=%s",
		username,
		password,
		c.Host,
		c.Port,
		c.SslMode,
		c.Database,
	)

	db, err := sqlx.Open("postgres", denodoConnStr)
	if err != nil {
		return nil, fmt.Errorf("sqlx.Open failed %s", err.Error())
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlx.Ping failed %s", err.Error())
	}
	time.Sleep(500 * time.Millisecond)
	client := Client{
		Conn: db,
	}
	return &client, nil
}

func (c *Client) ExecuteQuery(sqlStmt string) error {
	var err error
	_, err = c.Conn.Exec(sqlStmt)
	time.Sleep(500 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("Query Execution failed %s", err.Error())
	}
	return nil
}

func (c *Client) GetDatabasesFromVdp(targetDBs []string) (*[]models.GetDatabasesResult, error) {
	dbQuery, args, err := buildQueryToGetDatabases(targetDBs)
	if err != nil {
		return nil, fmt.Errorf("buildQueryToGetDatabases failed %s", err.Error())
	}
	getDatabasesResults := &[]models.GetDatabasesResult{}

	err = c.Conn.Select(getDatabasesResults, dbQuery, args...)
	time.Sleep(500 * time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return getDatabasesResults, nil
}

func (c *Client) GetViewsFromVdp(databaseName string) ([]models.GetViewsResult, error) {
	query := `select
                database_name
                , name
                , view_type
                , description
              from
                get_views()
              where
                database_name = $1`
	getViewsResults := &[]models.GetViewsResult{}

	err := c.Conn.Select(getViewsResults, query, databaseName)
	time.Sleep(500 * time.Millisecond)
	if err != nil || getViewsResults == nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return *getViewsResults, nil
}

func (c *Client) GetViewColumnsFromVdp(databaseName string) ([]models.GetViewColumnsResult, error) {
	query := `select
                gvc.database_name
                , gv.view_type
                , gvc.view_name
                , gvc.column_name
                , gvc.column_remarks
              from
                get_view_columns()  gvc
              inner join
                get_views() gv
              on
                gvc.database_name = gv.database_name
                and gvc.view_name = gv.name
			  where
                gvc.database_name = $1`
	getViewColumnsResults := &[]models.GetViewColumnsResult{}

	err := c.Conn.Select(getViewColumnsResults, query, databaseName)
	time.Sleep(500 * time.Millisecond)
	if err != nil || getViewColumnsResults == nil {
		return nil, fmt.Errorf("GetViewColumnsFromVdp failed %s", err.Error())
	}
	return *getViewColumnsResults, nil
}

func (c *Client) UpdateVdpDatabaseDesc(databaseName, description string) error {
	// Todo: use placeholder
	alterStatement := fmt.Sprintf(`alter database %s '%s'`, databaseName, escapeSingleQuoteInString(description))
	err := c.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpDatabaseDesc failed %s", err)
	}
	return nil
}

func (c *Client) UpdateVdpTableDesc(getViewResult models.GetViewsResult, description string) error {
	// Todo: use placeholder
	alterTableTarget := getAlterViewType(getViewResult.ViewType)
	alterStatement := fmt.Sprintf(`alter %s %s 
	                               description = '%s'`,
		alterTableTarget,
		getViewResult.ViewName,
		escapeSingleQuoteInString(description),
	)
	err := c.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpTableDesc failed %s", err)
	}
	return nil
}

func (c *Client) UpdateVdpTableColumnDesc(getViewColumnResult models.GetViewColumnsResult, description string) error {
	alterTableTarget := getAlterViewType(getViewColumnResult.ViewType)
	alterStatement := fmt.Sprintf(`alter %s %s (alter column %s add (description = '%s'))`,
		alterTableTarget,
		getViewColumnResult.ViewName,
		getViewColumnResult.ColumnName,
		escapeSingleQuoteInString(description),
	)
	err := c.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpTableColumnDesc failed %s", err)
	}
	return nil
}

func getAlterViewType(viewType int) string {
	var alterTableTarget string
	switch viewType {
	case 0: // base view
		alterTableTarget = "table"
	default: // 1: derived view, 2: interface view, 3: materialized view
		alterTableTarget = "view"
	}
	return alterTableTarget
}

func escapeSingleQuoteInString(input string) string {
	return strings.ReplaceAll(input, "'", "''")
}

func buildQueryToGetDatabases(targetDBList []string) (string, []interface{}, error) {
	var dbQuery string
	var args []interface{}
	if len(targetDBList) == 0 {
		dbQuery = `
		    select
				db_name
				, description
			from
				get_databases()`
	} else {
		dbQuery = `
			select
				db_name
				, description
			from
				get_databases()
			where
				db_name in (?)`
		var err error
		dbQuery, args, err = sqlx.In(dbQuery, targetDBList)
		if err != nil {
			return "", nil, err
		}
	}

	return dbQuery, args, nil
}
