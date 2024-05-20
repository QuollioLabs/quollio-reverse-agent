package denodo

import (
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/rest"
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
)

func (d *DenodoConnector) ReflectLocalDatabaseDescToDenodo(localDatabase models.Database, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[localDatabase.DatabaseName]; ok {
		if shouldUpdateDenodoLocalDatabase(localDatabase, qdcDBAsset) {
			putDatabaseInput := models.PutDatabaseInput{
				DatabaseID:      localDatabase.DatabaseId,
				Description:     qdcDBAsset.Description,
				DescriptionType: localDatabase.DescriptionType,
			}
			err := d.DenodoRepo.UpdateLocalDatabases(putDatabaseInput)
			if err != nil {
				code, denodoErr := rest.GetErrorCode(err)
				if denodoErr != nil {
					return err
				}
				switch code {
				case 401, 403:
					d.Logger.Warning("Update database description failed due to the ErrorCode %v Skip update. database name: %s.", code, localDatabase.DatabaseName)
				default:
					return err
				}
			}
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
		if shouldUpdateDenodoLocalTable(localViewDetail, tableAsset) {
			updateLocalViewInput := models.UpdateLocalViewInput{
				ID:              localViewDetail.Id,
				Description:     tableAsset.Description,
				DescriptionType: localViewDetail.Description,
			}
			err = d.DenodoRepo.UpdateLocalViewDescription(updateLocalViewInput)
			if err != nil {
				code, denodoErr := rest.GetErrorCode(err)
				if denodoErr != nil {
					return err
				}
				switch code {
				case 401, 403:
					d.Logger.Warning("Update table description failed due to the ErrorCode %v Skip update. database name: %s. table name: %s", code, localViewDetail.DatabaseName, localViewDetail.Name)
				default:
					return err
				}
			}
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
		localViewColumnMap := convertLocalColumnListToMap(localViewColumns)
		if localViewColumn, ok := localViewColumnMap[columnAssetName]; ok {
			if shouldUpdateDenodoLocalColumn(localViewColumn, columnAsset) {
				updateLocalViewColumnInput := models.UpdateLocalViewFieldInput{
					DatabaseName:     qdcDatabaseAsset.Name,
					FieldDescription: columnAsset.Description,
					FieldName:        localViewColumn.Name,
					ViewName:         qdcTableAsset.Name,
				}
				err = d.DenodoRepo.UpdateLocalViewFieldDescription(updateLocalViewColumnInput)
				if err != nil {
					code, denodoErr := rest.GetErrorCode(err)
					if denodoErr != nil {
						return err
					}
					switch code {
					case 401, 403:
						d.Logger.Warning("Update field description failed due to the ErrorCode %v Skip update. database name: %s. table name: %s column name: %s", code, qdcDatabaseAsset.Name, qdcTableAsset.Name, localViewColumn.Name)
					default:
						return err
					}
				}
				d.Logger.Debug("Update column description. database name: %s. table name: %s column name: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, localViewColumn.Name)
			}
		}
	}
	return nil
}

func shouldUpdateDenodoLocalDatabase(db models.Database, qdcDatabase qdc.Data) bool {
	if db.DatabaseDescription == "" && qdcDatabase.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoLocalTable(view models.ViewDetail, qdcTable qdc.Data) bool {
	if view.InLocal && view.Description == "" && qdcTable.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoLocalColumn(viewColumn models.ViewColumn, qdcColumn qdc.Data) bool {
	if viewColumn.InLocal && viewColumn.Description == "" && qdcColumn.Description != "" {
		return true
	}

	return false
}

func convertLocalColumnListToMap(localViewColumns []models.ViewColumn) map[string]models.ViewColumn {
	mapViewColumns := make(map[string]models.ViewColumn)
	for _, localViewColumn := range localViewColumns {
		mapViewColumns[localViewColumn.Name] = localViewColumn
	}
	return mapViewColumns
}
