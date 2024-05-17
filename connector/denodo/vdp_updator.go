package denodo

import (
	"quollio-reverse-agent/common/utils"
	dm "quollio-reverse-agent/models/denodo"
	"quollio-reverse-agent/repository/qdc"
)


func (d *DenodoConnector) ReflectVdpDatabaseDescToDenodo(getDatabaseResult dm.GetDatabasesResult, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[getDatabaseResult.DatabaseName]; ok {
		if !getDatabaseResult.Description.Valid && qdcDBAsset.Description != "" {
			d.UpdateVdpDatabaseDesc(getDatabaseResult.DatabaseName, qdcDBAsset.Description)
			d.Logger.Debug("Update Database description database name. database name: %s", getDatabaseResult.DatabaseName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpTableAttributeToDenodo(qdcTableAssets map[string]qdc.Data) error {
	for _, qdcTableAsset := range qdcTableAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(qdcTableAsset, "schema3")
		vdpTableAsset, err := d.GetViewFromVdp(qdcDatabaseAsset.Name, qdcTableAsset.PhysicalName)
		if err != nil {
			return err
		}
		if len(vdpTableAsset) == 0 {
			d.Logger.Debug("Skip ReflectVdpTableAttributeToDenodo. database name: %s. table name: %s", qdcDatabaseAsset.Name, qdcTableAsset.PhysicalName)
			continue
		}

		if !vdpTableAsset[0].Description.Valid && qdcTableAsset.Description != "" {
			d.UpdateVdpTableDesc(vdpTableAsset[0], qdcTableAsset.Description)
			d.Logger.Debug("Update table description. database name: %s. table name: %s", vdpTableAsset[0].DatabaseName, vdpTableAsset[0].ViewName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpColumnAttributeToDenodo(qdcColumnAssets map[string]qdc.Data) error {
	for _, qdcColumnAsset := range qdcColumnAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(qdcColumnAsset, "schema3")
		qdcTableAsset := utils.GetSpecifiedAssetFromPath(qdcColumnAsset, "table")
		vdpColumnAsset, err := d.GetViewColumnsFromVdp(qdcDatabaseAsset.Name, qdcTableAsset.Name)
		if err != nil {
			return err
		}
		if len(vdpColumnAsset) == 0 {
			d.Logger.Debug("Skip ReflectVdpColumnAttributeToDenodo. database name: %s. table name: %s. column name: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, qdcColumnAsset.PhysicalName)
			continue
		}

		if !vdpColumnAsset[0].ColumnRemarks.Valid && qdcColumnAsset.Description != "" {
			d.UpdateVdpTableColumnDesc(vdpColumnAsset[0], qdcColumnAsset.Description)
			d.Logger.Debug(
				"Update column description. database name: %s. table name: %s. column name: %s", vdpColumnAsset[0].DatabaseName, vdpColumnAsset[0].ViewName, vdpColumnAsset[0].ColumnName,
			)
		}
	}
	return nil
}
