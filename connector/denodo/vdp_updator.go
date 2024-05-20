package denodo

import (
	"fmt"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/odbc/models"
	"quollio-reverse-agent/repository/qdc"
)

func (d *DenodoConnector) ReflectVdpDatabaseDescToDenodo(getDatabaseResult models.GetDatabasesResult, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[getDatabaseResult.DatabaseName]; ok {
		if shouldUpdateDenodoVdpDatabase(getDatabaseResult, qdcDBAsset) {
			err := d.DenodoDBClient.UpdateVdpDatabaseDesc(getDatabaseResult.DatabaseName, qdcDBAsset.Description)
			if err != nil {
				return fmt.Errorf("ReflectVdpDatabaseDescToDenodo failed %s", err.Error())
			}
			d.Logger.Debug("Update Database description database name. database name: %s", getDatabaseResult.DatabaseName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpTableAttributeToDenodo(qdcTableAssets map[string]qdc.Data) error {
	for _, qdcTableAsset := range qdcTableAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(qdcTableAsset, "schema3")
		vdpTableAsset, err := d.DenodoDBClient.GetViewFromVdp(qdcDatabaseAsset.Name, qdcTableAsset.PhysicalName)
		if err != nil {
			return err
		}
		if len(vdpTableAsset) == 0 {
			d.Logger.Debug("Skip ReflectVdpTableAttributeToDenodo. database name: %s. table name: %s", qdcDatabaseAsset.Name, qdcTableAsset.PhysicalName)
			continue
		}

		if shouldUpdateDenodoVdpTable(vdpTableAsset[0], qdcTableAsset) {
			err := d.DenodoDBClient.UpdateVdpTableDesc(vdpTableAsset[0], qdcTableAsset.Description)
			if err != nil {
				return fmt.Errorf("ReflectVdpTableAttributeToDenodo failed %s", err.Error())
			}
			d.Logger.Debug("Update table description. database name: %s. table name: %s", vdpTableAsset[0].DatabaseName, vdpTableAsset[0].ViewName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpColumnAttributeToDenodo(qdcColumnAssets map[string]qdc.Data) error {
	for _, qdcColumnAsset := range qdcColumnAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(qdcColumnAsset, "schema3")
		qdcTableAsset := utils.GetSpecifiedAssetFromPath(qdcColumnAsset, "table")
		vdpColumnAsset, err := d.DenodoDBClient.GetViewColumnsFromVdp(qdcDatabaseAsset.Name, qdcTableAsset.Name)
		if err != nil {
			return err
		}
		if len(vdpColumnAsset) == 0 {
			d.Logger.Debug("Skip ReflectVdpColumnAttributeToDenodo. database name: %s. table name: %s. column name: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, qdcColumnAsset.PhysicalName)
			continue
		}

		if shouldUpdateDenodoVdpColumn(vdpColumnAsset[0], qdcColumnAsset) {
			err := d.DenodoDBClient.UpdateVdpTableColumnDesc(vdpColumnAsset[0], qdcColumnAsset.Description)
			if err != nil {
				return fmt.Errorf("UpdateVdpTableColumnDesc failed %s", err.Error())
			}
			d.Logger.Debug(
				"Update column description. database name: %s. table name: %s. column name: %s", vdpColumnAsset[0].DatabaseName, vdpColumnAsset[0].ViewName, vdpColumnAsset[0].ColumnName,
			)
		}
	}
	return nil
}

func shouldUpdateDenodoVdpDatabase(db models.GetDatabasesResult, qdcDatabase qdc.Data) bool {
	if !db.Description.Valid && qdcDatabase.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoVdpTable(view models.GetViewsResult, qdcTable qdc.Data) bool {
	if !view.Description.Valid && qdcTable.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoVdpColumn(viewColumn models.GetViewColumnsResult, qdcColumn qdc.Data) bool {
	if !viewColumn.ColumnRemarks.Valid && qdcColumn.Description != "" {
		return true
	}

	return false
}
