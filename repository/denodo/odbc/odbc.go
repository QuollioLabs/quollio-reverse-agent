package odbc

import (
	"fmt"
	"quollio-reverse-agent/repository/denodo/odbc/models"
	"strings"

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
	client := Client{
		Conn: db,
	}
	return &client, nil
}

func (c *Client) ExecuteQuery(sqlStmt string) error {
	var err error
	_, err = c.Conn.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("Query Execution failed %s", err.Error())
	}
	return nil
}

func (c *Client) GetDatabasesFromVdp() (*[]models.GetDatabasesResult, error) {
	dbQuery := `select
	              db_name
				  , description
				from
				  get_databases()`
	getDatabasesResults := &[]models.GetDatabasesResult{}

	err := c.Conn.Select(getDatabasesResults, dbQuery)
	if err != nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return getDatabasesResults, nil
}

func (c *Client) GetViewFromVdp(databaseName, viewName string) ([]models.GetViewsResult, error) {
	query := `select
                database_name
                , name
                , view_type
                , description
              from
                get_views()
              where
                database_name = $1
                and name = $2`
	getViewsResults := &[]models.GetViewsResult{}

	err := c.Conn.Select(getViewsResults, query, databaseName, viewName)
	if err != nil || getViewsResults == nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return *getViewsResults, nil
}

func (c *Client) GetViewColumnsFromVdp(databaseName, viewName string) ([]models.GetViewColumnsResult, error) {
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
                gvc.database_name = $1
                and gvc.view_name = $2`
	getViewColumnsResults := &[]models.GetViewColumnsResult{}

	err := c.Conn.Select(getViewColumnsResults, query, databaseName, viewName)
	if err != nil || getViewColumnsResults == nil {
		return nil, fmt.Errorf("GetViewColumnsFromVdp failed %s", err.Error())
	}
	return *getViewColumnsResults, nil
}

func (c *Client) UpdateVdpDatabaseDesc(databaseName, description string) error {
	alterStatement := fmt.Sprintf(`
	                    alter database %s 
	                    description = '%s'`, databaseName, escapeSingleQuoteInString(description))

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
	alterStatement := fmt.Sprintf(`alter %s %s 
	                               alter column %s add (description = '%s')`,
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
