package denodo

import (
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
)

func (d *DenodoConnector) ReflectLocalDatabaseDescToDenodo(localDatabase models.Database, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[localDatabase.DatabaseName]; ok {
		if localDatabase.DatabaseDescription != "" && qdcDBAsset.Description != "" {
			putDatabaseInput := models.PutDatabaseInput{
				DatabaseID:      localDatabase.DatabaseId,
				Description:     qdcDBAsset.Description,
				DescriptionType: localDatabase.DescriptionType,
			}
			d.DenodoRepo.UpdateLocalDatabases(putDatabaseInput)
			d.Logger.Debug("Update Database description database name. database name: %s", localDatabase.DatabaseName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectLocalTableAttributeToDenodo(tableAssets map[string]qdc.Data) error {
	for _, tableAsset := range tableAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(tableAsset, "schema3")
		localViewDetail, err := d.DenodoRepo.GetViewDetails(qdcDatabaseAsset.Name, tableAsset.PhysicalName)
		if err != nil {
			return err
		}
		if localViewDetail.Description == "" && tableAsset.Description != "" {
			updateLocalViewInput := models.UpdateLocalViewInput{
				ID:              localViewDetail.Id,
				Description:     tableAsset.Description,
				DescriptionType: localViewDetail.Description,
			}
			d.DenodoRepo.UpdateLocalViewDescription(updateLocalViewInput)
			d.Logger.Debug("Update table description. database name: %s. table name: %s", localViewDetail.DatabaseName, localViewDetail.Name)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectLocalColumnAttributeToDenodo(columnAssets map[string]qdc.Data) error {
	for columnAssetName, columnAsset := range columnAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(columnAsset, "schema3")
		qdcTableAsset := utils.GetSpecifiedAssetFromPath(columnAsset, "table")
		localViewColumns, err := d.DenodoRepo.GetViewColumns(qdcDatabaseAsset.Name, qdcTableAsset.Name)
		if err != nil {
			return err
		}
		localViewColumnMap := ConvertLocalColumnListToMap(localViewColumns)
		if localViewColumn, ok := localViewColumnMap[columnAssetName]; ok {
			if localViewColumn.Description == "" && columnAsset.Description != "" {
				updateLocalViewColumnInput := models.UpdateLocalViewFieldInput{
					DatabaseName:     qdcDatabaseAsset.Name,
					FieldDescription: columnAsset.Description,
					FieldName:        localViewColumn.Name,
					ViewName:         qdcTableAsset.Name,
				}
				d.DenodoRepo.UpdateLocalViewFieldDescription(updateLocalViewColumnInput)
				d.Logger.Debug("Update column description. database name: %s. table name: %s column name: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, localViewColumn.Name)
			}
		}
	}
	return nil
}
