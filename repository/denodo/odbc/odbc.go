package odbc

import (
	"fmt"

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

func (c *Client) GetQueryResults(target interface{}, query string, args ...interface{}) error {
	err := c.Conn.Select(target, query, args...)
	if err != nil {
		return fmt.Errorf("GetQueryResults failed %s", err.Error())
	}
	return nil
}
