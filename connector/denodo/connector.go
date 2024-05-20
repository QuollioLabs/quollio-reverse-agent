package denodo

import (
	"fmt"
	"os"

	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
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

func (d *DenodoConnector) ReflectMetadataToDataCatalog() error {
	d.Logger.Info("Get Denodo assets from QDIC")
	rootAssets, err := d.GetAllDenodoRootAssets()
	if err != nil {
		d.Logger.Error("Failed to GetAllDenodoRootAssets: %s", err.Error())
		return err
	}
	rootAssetsMap := convertQdcAssetListToMap(rootAssets)

	tableAssets, err := d.GetAllChildAssetsByID(rootAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	tableAssetsMap := convertQdcAssetListToMap(tableAssets)

	columnAssets, err := d.GetAllChildAssetsByID(tableAssets)
	if err != nil {
		d.Logger.Error("Failed to GetAllChildAssetsByID for tableAssets: %s", err.Error())
		return err
	}
	columnAssetsMap := convertQdcAssetListToMap(columnAssets)

	d.Logger.Info("Update Vdp assets metadata with qdic assets")
	err = d.ReflectVdpMetadataToDataCatalog(rootAssetsMap, tableAssetsMap, columnAssetsMap)
	if err != nil {
		return err
	}

	err = d.ReflectDenodoDataCatalogMetadataToDataCatalog(rootAssetsMap, tableAssetsMap, columnAssetsMap)
	if err != nil {
		return err
	}
	return nil
}

func (d *DenodoConnector) ReflectVdpMetadataToDataCatalog(qdcRootAssetsMap, qdcTableAssetsMap, qdcColumnAssetsMap map[string]qdc.Data) error {
	d.Logger.Info("Update Denodo database assets")
	vdpDatabases, err := d.DenodoDBClient.GetDatabasesFromVdp()
	if err != nil {
		return err
	}
	for _, vdpDatabase := range *vdpDatabases {
		err = d.ReflectVdpDatabaseDescToDenodo(vdpDatabase, qdcRootAssetsMap)
		if err != nil {
			d.Logger.Error("Failed to ReflectVdpDatabaseDescToDenodo: %s", err.Error())
			return err
		}
	}

	d.Logger.Info("Update Denodo table assets")
	err = d.ReflectVdpTableAttributeToDenodo(qdcTableAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectVdpTableAttributeToDenodo: %s", err.Error())
		return err
	}

	d.Logger.Info("Update Denodo column assets")
	err = d.ReflectVdpColumnAttributeToDenodo(qdcColumnAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectVdpColumnAttributeToDenodo: %s", err.Error())
		return err
	}

	return nil
}

func (d *DenodoConnector) ReflectDenodoDataCatalogMetadataToDataCatalog(qdcRootAssetsMap, qdcTableAssetsMap, qdcColumnAssetsMap map[string]qdc.Data) error {
	d.Logger.Info("List Denodo database assets")
	localDatabases, err := d.DenodoRepo.GetLocalDatabases()
	if err != nil {
		return err
	}
	for _, localDatabase := range localDatabases {
		err = d.ReflectLocalDatabaseDescToDenodo(localDatabase, qdcRootAssetsMap)
		if err != nil {
			d.Logger.Error("Failed to ReflectLocalDatabaseDescToDenodo: %s", err.Error())
			return err
		}
	}

	d.Logger.Info("List Denodo table assets")
	err = d.ReflectLocalTableAttributeToDenodo(qdcTableAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalTableAttributeToDenodo: %s", err.Error())
		return err
	}

	d.Logger.Info("List Denodo column assets")
	err = d.ReflectLocalColumnAttributeToDenodo(qdcColumnAssetsMap)
	if err != nil {
		d.Logger.Error("Failed to ReflectLocalColumnAttributeToDenodo: %s", err.Error())
		return err
	}

	return nil
}

func convertQdcAssetListToMap(qdcAssetList []qdc.Data) map[string]qdc.Data {
	mapQDCAsset := make(map[string]qdc.Data)
	for _, qdcAsset := range qdcAssetList {
		mapQDCAsset[qdcAsset.PhysicalName] = qdcAsset
	}
	return mapQDCAsset
}
