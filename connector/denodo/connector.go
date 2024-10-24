package denodo

import (
	"fmt"
	"os"
	"strings"

	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/odbc"
	"quollio-reverse-agent/repository/denodo/odbc/models"
	"quollio-reverse-agent/repository/denodo/rest"
	"quollio-reverse-agent/repository/qdc"

	"golang.org/x/exp/slices"
)

type DenodoConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	DenodoRepo           rest.DenodoRepo
	DenodoDBClient       *odbc.Client
	CompanyID            string
	DenodoHostName       string
	AssetCreatedBy       string
	OverwriteMode        string
	PrefixForUpdate      string
	DenodoQueryTargetDBs []string
	Logger               *logger.BuiltinLogger
}

func NewDenodoConnector(prefixForUpdate, overwriteMode string, logger *logger.BuiltinLogger) (DenodoConnector, error) {

	qdcBaseURL := os.Getenv("QDC_BASE_URL")
	qdcClientID := os.Getenv("QDC_CLIENT_ID")
	qdcClientSecret := os.Getenv("QDC_CLIENT_SECRET")
	assetCreatedBy := os.Getenv("QDC_ASSET_CREATED_BY")
	companyId := os.Getenv("COMPANY_ID")

	denodoClientID := os.Getenv("DENODO_CLIENT_ID")
	denodoClientSecret := os.Getenv("DENODO_CLIENT_SECRET")
	denodoHostName := os.Getenv("DENODO_HOST_NAME")
	denodoRestAPIPort := os.Getenv("DENODO_REST_API_PORT")
	denodoRestAPIBaseURL := fmt.Sprintf("https://%s:%s/denodo-data-catalog", denodoHostName, denodoRestAPIPort)

	denodoQueryTargetDB := os.Getenv("DENODO_QUERY_TARGET_DB")
	denodoQueryTargetList := utils.ConvertStringToListByWhiteSpace(denodoQueryTargetDB)

	denodoDBConfig := odbc.DenodoDBConfig{
		Database: os.Getenv("DENODO_DEFUALT_DB_NAME"),
		Host:     denodoHostName,
		Port:     os.Getenv("DENODO_ODBC_PORT"),
		SslMode:  "require",
	}
	client, err := denodoDBConfig.NewClient(denodoClientID, denodoClientSecret)
	if err != nil {
		return DenodoConnector{}, err
	}

	denodoRepo := rest.NewDenodoRepo(denodoClientID, denodoClientSecret, denodoRestAPIBaseURL)
	externalAPI, err := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret, logger)
	if err != nil {
		return DenodoConnector{}, fmt.Errorf("Failed to initialize QDCExternalAPI client in Denodo Connector %s", err)
	}
	connector := DenodoConnector{
		QDCExternalAPIClient: externalAPI,
		DenodoRepo:           *denodoRepo,
		DenodoDBClient:       client,
		CompanyID:            companyId,
		DenodoHostName:       denodoHostName,
		AssetCreatedBy:       assetCreatedBy,
		OverwriteMode:        overwriteMode,
		PrefixForUpdate:      prefixForUpdate,
		DenodoQueryTargetDBs: denodoQueryTargetList,
		Logger:               logger,
	}
	return connector, nil
}

func (d *DenodoConnector) ReflectMetadataToDataCatalog() error {
	defer d.DenodoDBClient.Conn.DB.Close()
	d.Logger.Info("Get Denodo assets from QDIC")
	rootAssets, err := d.QDCExternalAPIClient.GetAllRootAssets("denodo", d.AssetCreatedBy)
	if err != nil {
		d.Logger.Error("Failed to GetAllDenodoRootAssets: %s", err.Error())
		return err
	}
	// MEMO: Filter db assets by a parameter.
	targetRootAssets := getFilteredRootAssets(d.DenodoQueryTargetDBs, rootAssets)

	rootAssetsMap := convertQdcAssetListToMap(targetRootAssets)

	d.Logger.Info("Get table assets from schema assets")
	tableAssets, err := d.QDCExternalAPIClient.GetAllChildAssetsByID(targetRootAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	tableAssetsMap := convertQdcAssetListToMap(tableAssets)

	d.Logger.Info("Get column assets from table assets")
	columnAssets, err := d.QDCExternalAPIClient.GetAllChildAssetsByID(tableAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	columnAssetsMap := convertQdcAssetListToMap(columnAssets)

	d.Logger.Info("Start to ReflectVdpMetadataToDataCatalog. Will update VDP resources")
	err = d.ReflectVdpMetadataToDataCatalog(rootAssetsMap, tableAssetsMap, columnAssetsMap)
	if err != nil {
		return err
	}
	d.Logger.Info("Start to ReflectVdpMetadataToDataCatalog. Will update LocalCatalog resources")
	err = d.ReflectDenodoDataCatalogMetadataToDataCatalog(rootAssetsMap, tableAssetsMap, columnAssetsMap)
	if err != nil {
		return err
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpMetadataToDataCatalog(qdcRootAssetsMap, qdcTableAssetsMap, qdcColumnAssetsMap map[string]qdc.Data) error {
	d.Logger.Info("Start to update denodo vdp database assets")
	vdpDatabases, err := d.DenodoDBClient.GetDatabasesFromVdp(d.DenodoQueryTargetDBs)
	if err != nil {
		return err
	}
	for _, vdpDatabase := range *vdpDatabases {
		denodoDBConfig := odbc.DenodoDBConfig{
			Database: vdpDatabase.DatabaseName,
			Host:     os.Getenv("DENODO_HOST_NAME"),
			Port:     os.Getenv("DENODO_ODBC_PORT"),
			SslMode:  "require",
		}
		client, err := denodoDBConfig.NewClient(os.Getenv("DENODO_CLIENT_ID"), os.Getenv("DENODO_CLIENT_SECRET"))
		if err != nil {
			return err
		}
		d.DenodoDBClient = client
		d.Logger.Info("Connected to %s", vdpDatabase.DatabaseName)

		d.Logger.Info("Start to update denodo database assets")
		databaseGlobalID := utils.GetGlobalId(d.CompanyID, d.DenodoHostName, vdpDatabase.DatabaseName, "schema")
		if qdcDatabaseAsset, ok := qdcRootAssetsMap[databaseGlobalID]; ok {
			if qdcDatabaseAsset.IsLost {
				d.Logger.Debug("Skip database update because it is lost in qdc : %s", qdcDatabaseAsset.PhysicalName)
				continue
			}
			if shouldUpdateDenodoVdpDatabase(d.PrefixForUpdate, d.OverwriteMode, vdpDatabase, qdcDatabaseAsset) {
				descForUpdate := genUpdateString(qdcDatabaseAsset.LogicalName, qdcDatabaseAsset.Description)
				descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
				err := d.DenodoDBClient.UpdateVdpDatabaseDesc(vdpDatabase.DatabaseName, descWithPrefix)
				if err != nil {
					if isPrivilegesErr(err.Error()) {
						d.Logger.Warning("Failed to update DB due to permission problem. Error: %s, DB Name: %s", err.Error(), vdpDatabase.DatabaseName)
					} else {
						return err
					}
				}
				d.Logger.Debug("Updated database description. database name: %s.", vdpDatabase.DatabaseName)
			}
		}

		d.Logger.Info("Start to update denodo table assets")
		vdpTableAssets, err := d.DenodoDBClient.GetViewsFromVdp(vdpDatabase.DatabaseName)
		if err != nil {
			return err
		}
		for _, vdpTableAsset := range vdpTableAssets {
			tableFQN := fmt.Sprint(vdpDatabase.DatabaseName, vdpTableAsset.ViewName)
			tableGlobalID := utils.GetGlobalId(d.CompanyID, d.DenodoHostName, tableFQN, "table")
			d.Logger.Debug("Will update table if condition is true. GlobalID: %s. DBName: %s TableName: %s ", tableGlobalID, vdpTableAsset.DatabaseName, vdpTableAsset.ViewName)
			if qdcTableAsset, ok := qdcTableAssetsMap[tableGlobalID]; ok {
				if qdcTableAsset.IsLost {
					d.Logger.Debug("Skip table update because it is lost in qdc : %s", qdcTableAsset.PhysicalName)
					continue
				}
				if shouldUpdateDenodoVdpTable(d.PrefixForUpdate, d.OverwriteMode, vdpTableAsset, qdcTableAsset) {
					descForUpdate := genUpdateString(qdcTableAsset.LogicalName, qdcTableAsset.Description)
					descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
					err := d.DenodoDBClient.UpdateVdpTableDesc(vdpTableAsset, descWithPrefix)
					if err != nil {
						if isPrivilegesErr(err.Error()) {
							d.Logger.Warning("Failed to update Table due to permission problem. Error: %s, DB Name: %s Table Name: %s", err.Error(), vdpTableAsset.DatabaseName, vdpTableAsset.ViewName)
						} else {
							return err
						}
					}
					d.Logger.Debug("Updated table description. database name: %s. table name: %s", vdpTableAsset.DatabaseName, vdpTableAsset.ViewName)
				}
			}
		}
		d.Logger.Info("Start to update denodo column assets")
		vdpColumnAssets, err := d.DenodoDBClient.GetViewColumnsFromVdp(vdpDatabase.DatabaseName)
		if err != nil {
			return err
		}
		for _, vdpColumnAsset := range vdpColumnAssets {
			columnFQN := fmt.Sprint(vdpDatabase.DatabaseName, vdpColumnAsset.ViewName, vdpColumnAsset.ColumnName)
			columnGlobalID := utils.GetGlobalId(d.CompanyID, d.DenodoHostName, columnFQN, "column")
			if qdcColumnAsset, ok := qdcColumnAssetsMap[columnGlobalID]; ok {
				if qdcColumnAsset.IsLost {
					d.Logger.Debug("Skip column update because it is lost in qdc : %s", qdcColumnAsset.PhysicalName)
					continue
				}
				if vdpColumnAsset.ViewType != 1 {
					d.Logger.Debug("Skip update view. only derived view will be updated. database name: %s, table name: %s column name: %s", vdpColumnAsset.DatabaseName, vdpColumnAsset.ViewName, vdpColumnAsset.ColumnName)
					continue
				}
				if shouldUpdateDenodoVdpColumn(d.PrefixForUpdate, d.OverwriteMode, vdpColumnAsset, qdcColumnAsset) {
					d.Logger.Debug("Will update column. GlobalID: %s. DBName: %s TableName: %s ColumnName: %s", columnGlobalID, vdpColumnAsset.DatabaseName, vdpColumnAsset.ViewName, vdpColumnAsset.ColumnName)
					descForUpdate := genUpdateString(qdcColumnAsset.LogicalName, qdcColumnAsset.Description)
					descWithPrefix := utils.AddPrefixToStringIfNotHas(d.PrefixForUpdate, descForUpdate)
					err := d.DenodoDBClient.UpdateVdpTableColumnDesc(vdpColumnAsset, descWithPrefix)
					if err != nil {
						if isPrivilegesErr(err.Error()) {
							d.Logger.Warning("Failed to update Column due to permission problem. Error: %s, DB Name: %s Table Name: %s Column Name: %s", err.Error(), vdpColumnAsset.DatabaseName, vdpColumnAsset.ViewName, vdpColumnAsset.ColumnName)
						} else {
							return err
						}
					}
					d.Logger.Debug("Updated column description. database name: %s. table name: %s. column name: %s", vdpColumnAsset.DatabaseName, vdpColumnAsset.ViewName, vdpColumnAsset.ColumnName)
				}
			}
		}
		d.DenodoDBClient.Conn.Close()
	}
	return nil
}

func (d *DenodoConnector) ReflectDenodoDataCatalogMetadataToDataCatalog(qdcRootAssetsMap, qdcTableAssetsMap, qdcColumnAssetsMap map[string]qdc.Data) error {
	d.Logger.Info("Start to update denodo local database assets")
	localDatabases, err := d.DenodoRepo.GetLocalDatabases()
	if err != nil {
		return err
	}
	for _, localDatabase := range localDatabases {
		isSkipUpdateDatabaseByFilter := d.IsSkipUpdateDatabaseByFilter(localDatabase.DatabaseName)
		if isSkipUpdateDatabaseByFilter {
			d.Logger.Info("Skip ReflectLocalDatabaseDescToDenodo because %s is not contained targetDBList", localDatabase.DatabaseName)
			continue
		}
		d.Logger.Info("Start to run ReflectLocalDatabaseDescToDenodo")
		err = d.ReflectLocalDatabaseDescToDenodo(localDatabase, qdcRootAssetsMap)
		if err != nil {
			d.Logger.Error("Failed to ReflectLocalDatabaseDescToDenodo: %s", err.Error())
			return err
		}
	}

	d.Logger.Info("Start to update denodo table assets")
	err = d.ReflectLocalTableAttributeToDenodo(qdcTableAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalTableAttributeToDenodo: %s", err.Error())
		return err
	}

	d.Logger.Info("Start to update denodo column assets")
	err = d.ReflectLocalColumnAttributeToDenodo(qdcColumnAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalColumnAttributeToDenodo: %s", err.Error())
		return err
	}

	return nil
}

func (d *DenodoConnector) IsSkipUpdateDatabaseByFilter(targetDBName string) bool {
	if 1 <= len(d.DenodoQueryTargetDBs) {
		isDatabaseContained := slices.Contains(d.DenodoQueryTargetDBs, targetDBName)
		if !isDatabaseContained {
			return true
		}
	}
	return false
}

func convertQdcAssetListToMap(qdcAssetList []qdc.Data) map[string]qdc.Data {
	mapQDCAsset := make(map[string]qdc.Data)
	for _, qdcAsset := range qdcAssetList {
		mapQDCAsset[qdcAsset.ID] = qdcAsset
	}
	return mapQDCAsset
}

func shouldUpdateDenodoVdpDatabase(prefixForUpdate, overwriteMode string, db models.GetDatabasesResult, qdcDatabase qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcDatabase.Description != "" {
		return true
	}
	if !db.Description.Valid && qdcDatabase.Description != "" {
		return true
	}
	if (db.Description.Valid && db.Description.String == "") && qdcDatabase.Description != "" {
		return true
	}
	if db.Description.Valid && strings.HasPrefix(db.Description.String, prefixForUpdate) && qdcDatabase.Description != "" {
		return true
	}
	return false
}

func shouldUpdateDenodoVdpTable(prefixForUpdate, overwriteMode string, view models.GetViewsResult, qdcTable qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcTable.Description != "" {
		return true
	}
	if !view.Description.Valid && qdcTable.Description != "" {
		return true
	}
	if (view.Description.Valid && view.Description.String == "") && qdcTable.Description != "" {
		return true
	}
	if view.Description.Valid && strings.HasPrefix(view.Description.String, prefixForUpdate) && qdcTable.Description != "" {
		return true
	}
	return false
}

func shouldUpdateDenodoVdpColumn(prefixForUpdate, overwriteMode string, viewColumn models.GetViewColumnsResult, qdcColumn qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcColumn.Description != "" {
		return true
	}
	if !viewColumn.ColumnRemarks.Valid && qdcColumn.Description != "" {
		return true
	}
	if (viewColumn.ColumnRemarks.Valid && viewColumn.ColumnRemarks.String == "") && qdcColumn.Description != "" {
		return true
	}
	if viewColumn.ColumnRemarks.Valid && strings.HasPrefix(viewColumn.ColumnRemarks.String, prefixForUpdate) && qdcColumn.Description != "" {
		return true
	}
	return false
}

func genUpdateString(logicalName, description string) string {
	s := fmt.Sprintf("【項目名称】%s\n【説明】%s", logicalName, description)
	return s
}

func getFilteredRootAssets(targetDBs []string, qdcRootAssets []qdc.Data) []qdc.Data {
	var targetRootAssets []qdc.Data
	if 1 <= len(targetDBs) {
		var filteredRootAssets []qdc.Data
		for _, rootAsset := range qdcRootAssets {
			isContained := slices.Contains(targetDBs, rootAsset.PhysicalName)
			if isContained {
				filteredRootAssets = append(filteredRootAssets, rootAsset)
			}
		}
		targetRootAssets = filteredRootAssets
	} else {
		targetRootAssets = qdcRootAssets
	}
	return targetRootAssets
}

func isPrivilegesErr(errMessage string) bool {
	return strings.Contains(errMessage, "The user does not have enough privileges")
}
