package denodo

import (
	"fmt"
	dm "quollio-reverse-agent/models/denodo"
)

func (d *DenodoConnector) GetDatabasesFromVdp() (*[]dm.GetDatabasesResult, error) {
	dbQuery := `select
	              db_name
				  , description
				from
				  get_databases()`
	getDatabasesResults := &[]dm.GetDatabasesResult{}

	err := d.DenodoDBClient.GetQueryResults(getDatabasesResults, dbQuery)
	if err != nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return getDatabasesResults, nil
}

func (d *DenodoConnector) GetViewFromVdp(databaseName, viewName string) ([]dm.GetViewsResult, error) {
	query := fmt.Sprintf(`
	              select
	                database_name
					, name
					, view_type
					, description
				  from
				    get_views()
				  where
				    database_name = '%s'
					and name = '%s'`, databaseName, viewName)
	getViewsResults := &[]dm.GetViewsResult{}

	err := d.DenodoDBClient.GetQueryResults(getViewsResults, query)
	if err != nil || getViewsResults == nil {
		return nil, fmt.Errorf("GetDatabasesFromVdp failed %s", err.Error())
	}
	return *getViewsResults, nil
}

func (d *DenodoConnector) GetViewColumnsFromVdp(databaseName, viewName string) ([]dm.GetViewColumnsResult, error) {
	query := fmt.Sprintf(`
	              select
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
				    gvc.database_name = '%s'
					and gvc.view_name = '%s'
					`, databaseName, viewName)
	getViewColumnsResults := &[]dm.GetViewColumnsResult{}

	err := d.DenodoDBClient.GetQueryResults(getViewColumnsResults, query)
	if err != nil || getViewColumnsResults == nil {
		return nil, fmt.Errorf("GetViewColumnsFromVdp failed %s", err.Error())
	}
	return *getViewColumnsResults, nil
}

func (d *DenodoConnector) UpdateVdpDatabaseDesc(databaseName, description string) error {
	alterStatement := fmt.Sprintf(`
	                    alter database %s 
	                    description = '%s'`, databaseName, description)

	err := d.DenodoDBClient.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpDatabaseDesc failed %s", err)
	}
	return nil
}

func (d *DenodoConnector) UpdateVdpTableDesc(getViewResult dm.GetViewsResult, description string) error {
	alterTableTarget := getAlterViewType(getViewResult.ViewType)
	alterStatement := fmt.Sprintf(`alter %s %s 
	                               description = '%s'`,
		alterTableTarget,
		getViewResult.ViewName,
		description,
	)
	err := d.DenodoDBClient.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpTableDesc failed %s", err)
	}
	return nil
}

func (d *DenodoConnector) UpdateVdpTableColumnDesc(getViewColumnResult dm.GetViewColumnsResult, description string) error {
	alterTableTarget := getAlterViewType(getViewColumnResult.ViewType)
	alterStatement := fmt.Sprintf(`alter %s %s 
	                               alter column %s add (description = '%s')`,
		alterTableTarget,
		getViewColumnResult.ViewName,
		getViewColumnResult.ColumnName,
		description,
	)
	err := d.DenodoDBClient.ExecuteQuery(alterStatement)
	if err != nil {
		return fmt.Errorf("UpdateVdpTableColumnDesc failed %s", err)
	}
	return nil
}
