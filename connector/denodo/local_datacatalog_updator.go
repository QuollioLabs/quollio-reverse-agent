package denodo

import (
	"fmt"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/rest"
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
	"strings"
)

func (d *DenodoConnector) ReflectLocalDatabaseDescToDenodo(localDatabase models.Database, dbAssets map[string]qdc.Data) error {
	databaseGlobalID := utils.GetGlobalId(d.CompanyID, d.DenodoHostName, localDatabase.DatabaseName, "schema")
	if qdcDBAsset, ok := dbAssets[databaseGlobalID]; ok {
		if qdcDBAsset.IsLost {
			d.Logger.Debug("Skip db update because it is lost in qdc : %s", qdcDBAsset.PhysicalName)
			return nil
		}

		if shouldUpdateDenodoLocalDatabase(d.PrefixForUpdate, d.OverwriteMode, localDatabase, qdcDBAsset) {
			descForUpdate := genUpdateString(qdcDBAsset.LogicalName, qdcDBAsset.Description)
			descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
			putDatabaseInput := models.PutDatabaseInput{
				DatabaseID:      localDatabase.DatabaseId,
				Description:     descWithPrefix,
				DescriptionType: "RICH_TEXT",
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
			d.Logger.Debug("Updated Database description database name. database name: %s", localDatabase.DatabaseName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectLocalTableAttributeToDenodo(tableAssets map[string]qdc.Data) error {
	for _, tableAsset := range tableAssets {
		qdcDatabaseAsset := qdc.GetSpecifiedAssetFromPath(tableAsset, "schema3")
		if tableAsset.IsLost {
			d.Logger.Debug("Skip table update because it is lost in qdc : %s->%s", qdcDatabaseAsset.Name, tableAsset.PhysicalName)
			continue
		}
		isSkipUpdateDatabaseByFilter := d.IsSkipUpdateDatabaseByFilter(qdcDatabaseAsset.Name)
		if isSkipUpdateDatabaseByFilter {
			d.Logger.Info("Skip ReflectLocalTableAttributeToDenodo because %s is not contained targetDBList", qdcDatabaseAsset.Name)
			continue
		}

		if utils.IsStringContainJapanese(qdcDatabaseAsset.Name) || utils.IsStringContainJapanese(tableAsset.PhysicalName) {
			d.Logger.Warning("Skip to update table because API doesn't allow japanese letter as an input. Database: %s, Table: %s", qdcDatabaseAsset.Name, tableAsset.PhysicalName)
			continue
		}
		if !qdc.IsAssetContainsValueAsDescription(tableAsset) {
			d.Logger.Debug("Skip GetViewDetail and Update View because the description of qdc table asset is empty. Database: %s, Table: %s ", qdcDatabaseAsset.Name, tableAsset.PhysicalName)
			continue
		}
		localViewDetail, err := d.DenodoRepo.GetViewDetails(qdcDatabaseAsset.Name, tableAsset.PhysicalName)
		if err != nil {
			return fmt.Errorf("Failed to GetViewDetails. err: %s", err.Error())
		}
		if shouldUpdateDenodoLocalTable(d.PrefixForUpdate, d.OverwriteMode, localViewDetail, tableAsset) {
			descForUpdate := genUpdateString(tableAsset.LogicalName, tableAsset.Description)
			descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
			updateLocalViewInput := models.UpdateLocalViewInput{
				ID:              localViewDetail.Id,
				Description:     descWithPrefix,
				DescriptionType: "RICH_TEXT",
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
			d.Logger.Debug("Updated table description. database name: %s. table name: %s", localViewDetail.DatabaseName, localViewDetail.Name)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectLocalColumnAttributeToDenodo(columnAssets map[string]qdc.Data) error {
	for _, columnAsset := range columnAssets {
		qdcDatabaseAsset := qdc.GetSpecifiedAssetFromPath(columnAsset, "schema3")
		qdcTableAsset := qdc.GetSpecifiedAssetFromPath(columnAsset, "table")
		if columnAsset.IsLost {
			d.Logger.Debug("Skip column update because it is lost in qdc : %s->%s->%s", qdcDatabaseAsset.Name, qdcTableAsset.Name, columnAsset.PhysicalName)
			continue
		}

		isSkipUpdateDatabaseByFilter := d.IsSkipUpdateDatabaseByFilter(qdcDatabaseAsset.Name)
		if isSkipUpdateDatabaseByFilter {
			d.Logger.Info("Skip ReflectLocalColumnAttributeToDenodo because %s is not contained targetDBList", qdcDatabaseAsset.Name)
			continue
		}
		if utils.IsStringContainJapanese(qdcDatabaseAsset.Name) || utils.IsStringContainJapanese(qdcTableAsset.Name) {
			d.Logger.Warning("Skip to update table because API doesn't allow japanese letter as an input. Database: %s, Table: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name)
			continue
		}
		if !qdc.IsAssetContainsValueAsDescription(columnAsset) {
			d.Logger.Debug("Skip GetViewColumns and Update View Column because the description of qdc column asset is empty. Database: %s, Table: %s, Column:  %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, columnAsset.PhysicalName)
			continue
		}
		localViewColumns, err := d.DenodoRepo.GetViewColumns(qdcDatabaseAsset.Name, qdcTableAsset.Name)
		if err != nil {
			return err
		}
		localViewColumnMap := convertLocalColumnListToMap(localViewColumns)
		if localViewColumn, ok := localViewColumnMap[columnAsset.PhysicalName]; ok {
			if shouldUpdateDenodoLocalColumn(d.PrefixForUpdate, d.OverwriteMode, localViewColumn, columnAsset) {
				descForUpdate := genUpdateString(columnAsset.LogicalName, columnAsset.Description)
				descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
				updateLocalViewColumnInput := models.UpdateLocalViewFieldInput{
					DatabaseName:     qdcDatabaseAsset.Name,
					FieldDescription: descWithPrefix,
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
				d.Logger.Debug("Updated column description. database name: %s. table name: %s column name: %s", qdcDatabaseAsset.Name, qdcTableAsset.Name, localViewColumn.Name)
			}
		}
	}
	return nil
}

func shouldUpdateDenodoLocalDatabase(prefixForUpdate, overwriteMode string, db models.Database, qdcDatabase qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcDatabase.Description != "" {
		return true
	}

	if db.DatabaseDescription == "" && qdcDatabase.Description != "" {
		return true
	}

	if strings.HasPrefix(db.DatabaseDescription, prefixForUpdate) && qdcDatabase.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoLocalTable(prefixForUpdate, overwriteMode string, view models.ViewDetail, qdcTable qdc.Data) bool {
	if !view.InLocal {
		return false
	}
	if overwriteMode == utils.OverwriteAll && qdcTable.Description != "" {
		return true
	}

	if view.Description == "" && qdcTable.Description != "" {
		return true
	}

	if strings.HasPrefix(view.Description, prefixForUpdate) && qdcTable.Description != "" {
		return true
	}

	return false
}

func shouldUpdateDenodoLocalColumn(prefixForUpdate, overwriteMode string, viewColumn models.ViewColumn, qdcColumn qdc.Data) bool {
	if !viewColumn.InLocal {
		return false
	}
	if overwriteMode == utils.OverwriteAll && qdcColumn.Description != "" {
		return true
	}

	if viewColumn.Description == "" && qdcColumn.Description != "" {
		return true
	}

	if strings.HasPrefix(viewColumn.Description, prefixForUpdate) && qdcColumn.Description != "" {
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
