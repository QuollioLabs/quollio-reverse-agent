package denodo

import (
	"fmt"
	"os"

	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	dm "quollio-reverse-agent/models/denodo"
	"quollio-reverse-agent/repository/denodo/model"
	"quollio-reverse-agent/repository/denodo/odbc"
	"quollio-reverse-agent/repository/denodo/rest"
	"quollio-reverse-agent/repository/qdc"
)

type DenodoConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	DenodoRepo           rest.DenodoRepo
	DenodoDBClient       *odbc.Client
	Logger               *logger.BuiltinLogger
}

func NewDenodoConnector(logger *logger.BuiltinLogger) (DenodoConnector, error) {

	qdcBaseURL := os.Getenv("QDC_BASE_URL")
	qdcClientID := os.Getenv("QDC_CLIENT_ID")
	qdcClientSecret := os.Getenv("QDC_CLIENT_SECRET")

	denodoClientID := os.Getenv("DENODO_CLIENT_ID")
	denodoClientSecret := os.Getenv("DENODO_CLIENT_SECRET")
	denodoHostName := os.Getenv("DENODO_HOST_NAME")
	denodoBaseURL := fmt.Sprintf("https://%s", denodoHostName)

	denodoDBConfig := odbc.DenodoDBConfig{
		Database: os.Getenv("DENODO_DEFUALT_DB_NAME"),
		Host:     denodoHostName,
		Port:     os.Getenv("DENODO_ODBC_PORT"),
		SslMode:  "require",
	}
	client, err := denodoDBConfig.NewClient(qdcClientID, qdcClientSecret)
	if err != nil {
		return DenodoConnector{}, err
	}

	denodoRepo := rest.NewDenodoRepo(denodoClientID, denodoClientSecret, denodoBaseURL)
	externalAPI := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret)
	connector := DenodoConnector{
		QDCExternalAPIClient: externalAPI,
		DenodoRepo:           *denodoRepo,
		DenodoDBClient:       client,
		Logger:               logger,
	}
	return connector, nil
}

func (d *DenodoConnector) GetAllDenodoRootAssets() ([]qdc.Data, error) {
	var rootAssets []qdc.Data

	var lastAssetID string
	for {
		assetResponse, err := d.QDCExternalAPIClient.GetAssetByType("schema", lastAssetID)
		if err != nil {
			d.Logger.Error("Failed to GetAssetByType. lastAssetID: %s", lastAssetID)
			return nil, err
		}
		for _, assetData := range assetResponse.Data {
			switch assetData.ServiceName {
			case "denodo":
				rootAssets = append(rootAssets, assetData)
			default:
				continue
			}
		}
		switch assetResponse.LastID {
		case "":
			return rootAssets, nil
		default:
			d.Logger.Debug("GetAllDenodoRootAssets will continue. lastAssetID: %s", lastAssetID)
			lastAssetID = assetResponse.LastID
		}
	}
}

func (d *DenodoConnector) GetAllChildAssetsByID(parentAssets []qdc.Data) ([]qdc.Data, error) {
	var childAssets []qdc.Data

	for _, parentAsset := range parentAssets {
		childAssetIdChunks := utils.SplitArrayToChunks(parentAsset.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
		for _, childAssetIdChunk := range childAssetIdChunks {
			assets, err := d.QDCExternalAPIClient.GetAssetByIDs(childAssetIdChunk)
			if err != nil {
				return nil, err
			}
			childAssets = append(childAssets, assets.Data...)
		}
	}
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		d.Logger.Debug("The number of child assets is %v", len(childAssets))
		var childAssetIds []string
		for _, childAsset := range childAssets {
			childAssetIds = append(childAssetIds, childAsset.ID)
		}
		d.Logger.Debug("The child asset ids are %v", childAssetIds)
	}
	return childAssets, nil
}

func (d *DenodoConnector) ReflectVdpDatabaseDescToDenodo(getDatabaseResult dm.GetDatabasesResult, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[getDatabaseResult.DatabaseName]; ok {
		if !getDatabaseResult.Description.Valid && qdcDBAsset.Description != "" {
			d.UpdateVdpDatabaseDesc(getDatabaseResult.DatabaseName, qdcDBAsset.Description)
			d.Logger.Debug("Update Database description database name. database name: %s", getDatabaseResult.DatabaseName)
		}
	}
	return nil
}

func (d *DenodoConnector) ReflectLocalDatabaseDescToDenodo(localDatabase rest.Database, dbAssets map[string]qdc.Data) error {
	if qdcDBAsset, ok := dbAssets[localDatabase.DatabaseName]; ok {
		if localDatabase.DatabaseDescription != "" && qdcDBAsset.Description != "" {
			putDatabaseInput := rest.PutDatabaseInput{
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

func (d *DenodoConnector) ReflectLocalTableAttributeToDenodo(tableAssets map[string]qdc.Data) error {
	for _, tableAsset := range tableAssets {
		qdcDatabaseAsset := utils.GetSpecifiedAssetFromPath(tableAsset, "schema3")
		localViewDetail, err := d.DenodoRepo.GetViewDetails(qdcDatabaseAsset.Name, tableAsset.PhysicalName)
		if err != nil {
			return err
		}
		if localViewDetail.Description == "" && tableAsset.Description != "" {
			updateLocalViewInput := rest.UpdateLocalViewInput{
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
				updateLocalViewColumnInput := rest.UpdateLocalViewFieldInput{
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

func (d *DenodoConnector) ReflectMetadataToDataCatalog() error {
	err := d.ReflectVdpMetadataToDataCatalog()
	if err != nil {
		return err
	}

	err = d.ReflectDenodoDataCatalogMetadataToDataCatalog()
	if err != nil {
		return err
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpMetadataToDataCatalog() error {
	d.Logger.Info("List Denodo database assets")
	rootAssets, err := d.GetAllDenodoRootAssets()
	if err != nil {
		d.Logger.Error("Failed to GetAllDenodoRootAssets: %s", err.Error())
		return err
	}
	vdpDatabases, err := d.GetDatabasesFromVdp()
	if err != nil {
		return err
	}
	qdcDatabaseAssetMap := ConvertQdcAssetListToMap(rootAssets)
	for _, vdpDatabase := range *vdpDatabases {
		err = d.ReflectVdpDatabaseDescToDenodo(vdpDatabase, qdcDatabaseAssetMap)
		if err != nil {
			d.Logger.Error("Failed to ReflectVdpDatabaseDescToDenodo: %s", err.Error())
			return err
		}
	}

	d.Logger.Info("List Denodo table assets")
	tableAssets, err := d.GetAllChildAssetsByID(rootAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	qdcTableAssetMap := ConvertQdcAssetListToMap(tableAssets)
	err = d.ReflectVdpTableAttributeToDenodo(qdcTableAssetMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectVdpTableAttributeToDenodo: %s", err.Error())
		return err
	}

	d.Logger.Info("List Denodo column assets")
	columnAssets, err := d.GetAllChildAssetsByID(tableAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	qdcColumnAssetMap := ConvertQdcAssetListToMap(columnAssets)
	err = d.ReflectVdpColumnAttributeToDenodo(qdcColumnAssetMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectVdpColumnAttributeToDenodo: %s", err.Error())
		return err
	}

	return nil
}

func (d *DenodoConnector) ReflectDenodoDataCatalogMetadataToDataCatalog() error {
	d.Logger.Info("List Denodo database assets")
	rootAssets, err := d.GetAllDenodoRootAssets()
	if err != nil {
		d.Logger.Error("Failed to GetAllDenodoRootAssets: %s", err.Error())
		return err
	}
	localDatabases, err := d.DenodoRepo.GetLocalDatabases()
	if err != nil {
		return err
	}
	qdcDatabaseAssetMap := ConvertQdcAssetListToMap(rootAssets)
	for _, localDatabase := range localDatabases {
		err = d.ReflectLocalDatabaseDescToDenodo(localDatabase, qdcDatabaseAssetMap)
		if err != nil {
			d.Logger.Error("Failed to ReflectVdpDatabaseDescToDenodo: %s", err.Error())
			return err
		}
	}

	d.Logger.Info("List Denodo table assets")
	tableAssets, err := d.GetAllChildAssetsByID(rootAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	qdcTableAssetMap := ConvertQdcAssetListToMap(tableAssets)
	err = d.ReflectLocalTableAttributeToDenodo(qdcTableAssetMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalTableAttributeToDenodo: %s", err.Error())
		return err
	}

	d.Logger.Info("List Denodo column assets")
	columnAssets, err := d.GetAllChildAssetsByID(tableAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	qdcColumnAssetMap := ConvertQdcAssetListToMap(columnAssets)
	err = d.ReflectLocalColumnAttributeToDenodo(qdcColumnAssetMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalColumnAttributeToDenodo: %s", err.Error())
		return err
	}

	return nil
}

func ConvertQdcAssetListToMap(qdcAssetList []qdc.Data) map[string]qdc.Data {
	mapQDCAsset := make(map[string]qdc.Data)
	for _, qdcAsset := range qdcAssetList {
		mapQDCAsset[qdcAsset.PhysicalName] = qdcAsset
	}
	return mapQDCAsset
}

func ConvertLocalColumnListToMap(localViewColumns []model.ViewColumn) map[string]model.ViewColumn {
	mapViewColumns := make(map[string]model.ViewColumn)
	for _, localViewColumn := range localViewColumns {
		mapViewColumns[localViewColumn.Name] = localViewColumn
	}
	return mapViewColumns
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
