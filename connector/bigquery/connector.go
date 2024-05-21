package bigquery

import (
	"fmt"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/bigquery"
	"quollio-reverse-agent/repository/dataplex"
	"quollio-reverse-agent/repository/qdc"

	bq "cloud.google.com/go/bigquery"
)

type BigQueryConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	DataplexRepo         dataplex.DataplexClient
	BigQueryRepo         bigquery.BigQueryClient
	Logger               *logger.BuiltinLogger
}

func NewBigqueryConnector(logger *logger.BuiltinLogger) (BigQueryConnector, error) {
	serviceCreds := os.Getenv("GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS")
	dataplexClient, err := dataplex.NewDataplexClient(serviceCreds)
	if err != nil {
		return BigQueryConnector{}, err
	}

	bigqueryClient, err := bigquery.NewBigQueryClient(serviceCreds)
	if err != nil {
		return BigQueryConnector{}, err
	}
	qdcBaseURL := os.Getenv("QDC_BASE_URL")
	qdcClientID := os.Getenv("QDC_CLIENT_ID")
	qdcClientSecret := os.Getenv("QDC_CLIENT_SECRET")
	externalAPI := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret)
	connector := BigQueryConnector{
		QDCExternalAPIClient: externalAPI,
		DataplexRepo:         dataplexClient,
		BigQueryRepo:         bigqueryClient,
		Logger:               logger,
	}

	return connector, nil
}

func (b *BigQueryConnector) GetAllBigQueryRootAssets() ([]qdc.Data, error) {
	var rootAssets []qdc.Data

	var lastAssetID string
	for {
		assetResponse, err := b.QDCExternalAPIClient.GetAssetByType("schema", lastAssetID)
		if err != nil {
			b.Logger.Error("Failed to GetAssetByType. lastAssetID: %s", lastAssetID)
			return nil, err
		}
		for _, assetData := range assetResponse.Data {
			switch assetData.ServiceName {
			case "bigquery":
				rootAssets = append(rootAssets, assetData)
			default:
				continue
			}
		}
		switch assetResponse.LastID {
		case "":
			return rootAssets, nil
		default:
			b.Logger.Debug("GetAllBigQueryRootAssets will continue. lastAssetID: %s", lastAssetID)
			lastAssetID = assetResponse.LastID
		}
	}
}

func (b *BigQueryConnector) GetAllChildAssetsByID(parentAssets []qdc.Data) ([]qdc.Data, error) {
	var childAssets []qdc.Data

	for _, parentAsset := range parentAssets {
		childAssetIdChunks := utils.SplitArrayToChunks(parentAsset.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
		for _, childAssetIdChunk := range childAssetIdChunks {
			assets, err := b.QDCExternalAPIClient.GetAssetByIDs(childAssetIdChunk)
			if err != nil {
				return nil, err
			}
			childAssets = append(childAssets, assets.Data...)
		}
	}
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		b.Logger.Debug("The number of child assets is %v", len(childAssets))
		var childAssetIds []string
		for _, childAsset := range childAssets {
			childAssetIds = append(childAssetIds, childAsset.ID)
		}
		b.Logger.Debug("The child asset ids are %v", childAssetIds)
	}
	return childAssets, nil
}

func (b *BigQueryConnector) GetChildAssetsByParentAsset(assets qdc.Data) ([]qdc.Data, error) {
	var childAssets []qdc.Data

	childAssetIdChunks := utils.SplitArrayToChunks(assets.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
	for _, childAssetIdChunk := range childAssetIdChunks {
		assets, err := b.QDCExternalAPIClient.GetAssetByIDs(childAssetIdChunk)
		if err != nil {
			return nil, err
		}
		childAssets = append(childAssets, assets.Data...)
	}
	b.Logger.Debug("The number of child asset chunks is %v", len(childAssets))
	return childAssets, nil
}

func (b *BigQueryConnector) ReflectDatasetDescToBigQuery(schemaAssets []qdc.Data) error {
	for _, schemaAsset := range schemaAssets {
		datasetMetadata, err := b.BigQueryRepo.GetDatasetMetadata(schemaAsset.PhysicalName)
		if err != nil {
			b.Logger.Error("Failed to GetDatasetMetadata. : %s", schemaAsset.PhysicalName)
			return err
		}
		if datasetMetadata.Description == "" && schemaAsset.Description != "" {
			descWithPrefix := utils.AddQDICToStringAsPrefix(schemaAsset.Description)
			_, err = b.BigQueryRepo.UpdateDatasetDescription(schemaAsset.PhysicalName, descWithPrefix)
			if err != nil {
				b.Logger.Error("The update was failed.: %s", schemaAsset.PhysicalName)
				return err
			}
			b.Logger.Debug("The description of the asset was updated.: %s", schemaAsset.PhysicalName)
		}
	}
	return nil
}

func (b *BigQueryConnector) ReflectTableAttributeToBigQuery(tableAssets []qdc.Data) error {
	for _, tableAsset := range tableAssets {
		projectAsset := GetSpecifiedAssetFromPath(tableAsset, "schema4")
		datasetAsset := GetSpecifiedAssetFromPath(tableAsset, "schema3")
		var metadataToUpdate bq.TableMetadataToUpdate

		tableMetadata, err := b.BigQueryRepo.GetTableMetadata(datasetAsset.Name, tableAsset.PhysicalName)
		if err != nil {
			b.Logger.Error("Failed to GetTableMetadata: %s", tableMetadata.Name)
			return err
		}

		columnAssets, err := b.GetChildAssetsByParentAsset(tableAsset)
		if err != nil {
			b.Logger.Error("Failed to GetChildAssetsByParentAsset: %s", tableMetadata.Name)
			return err
		}

		tableSchemas, shouldSchemaUpdated := GetDescUpdatedSchema(columnAssets, tableMetadata)
		if shouldSchemaUpdated {
			metadataToUpdate.Schema = tableSchemas
			// Update table and schema description
			_, err = b.BigQueryRepo.UpdateTableMetadata(datasetAsset.Name, tableAsset.PhysicalName, metadataToUpdate)
			if err != nil {
				b.Logger.Error("Failed to UpdateTableMetadata: %s", tableAsset.PhysicalName)
				return err
			}
			b.Logger.Debug("The schema fields of table asset was updated.: %s", tableAsset.PhysicalName)
		}

		// Update table overview
		bqTableFQN := fmt.Sprintf("bigquery:%s.%s.%s", projectAsset.Name, datasetAsset.Name, tableAsset.PhysicalName)
		tableAssetEntry, err := b.DataplexRepo.LookupEntry(bqTableFQN, projectAsset.Name, tableMetadata.Location)
		if err != nil {
			b.Logger.Error("Failed to LookupEntry.: %s", tableAsset.PhysicalName)
			return err
		}
		if tableAssetEntry.BusinessContext.EntryOverview.Overview == "" && tableAsset.Description != "" {
			b.Logger.Debug("The overview of table asset will be updated.: %s", tableAsset.PhysicalName)
			descWithPrefix := utils.AddQDICToStringAsPrefix(tableAsset.Description)
			_, err := b.DataplexRepo.ModifyEntryOverview(tableAssetEntry.Name, descWithPrefix)
			if err != nil {
				b.Logger.Error("The update for the overview of the table asset was failed.: %s", tableAsset.PhysicalName)
				return err
			}
			b.Logger.Debug("The update for the overview of the table asset was succeeded.: %s", tableAsset.PhysicalName)
		}
	}
	return nil
}

func (b *BigQueryConnector) ReflectMetadataToDataCatalog() error {
	b.Logger.Info("List BigQuery project assets")
	rootAssets, err := b.GetAllBigQueryRootAssets()
	if err != nil {
		b.Logger.Error("Failed to GetAllBigQueryRootAssets: %s", err.Error())
		return err
	}

	b.Logger.Info("List BigQuery schema assets")
	schemaAssets, err := b.GetAllChildAssetsByID(rootAssets)
	if err != nil {
		b.Logger.Error("Failed to GetAllChildAssetsByID for schemaAssets: %s", err.Error())
		return err
	}

	err = b.ReflectDatasetDescToBigQuery(schemaAssets)
	if err != nil {
		b.Logger.Error("Failed to ReflectDatasetDescToBigQuery for schemaAssets: %s", err.Error())
		return err
	}

	b.Logger.Info("List BigQuery table assets")
	tableAssets, err := b.GetAllChildAssetsByID(schemaAssets)
	if err != nil {
		b.Logger.Error("Failed to GetAllChildAssetsByID: %s", err.Error())
		return err
	}

	err = b.ReflectTableAttributeToBigQuery(tableAssets)
	if err != nil {
		b.Logger.Error("Failed to ReflectTableAttributeToBigQuery: %s", err.Error())
		return err
	}
	return nil
}

func GetSpecifiedAssetFromPath(asset qdc.Data, pathLayer string) qdc.Path {
	path := asset.Path
	for _, p := range path {
		if p.PathLayer == pathLayer {
			return p
		}
	}
	return qdc.Path{}
}

func MapColumnAssetByColumnName(columnAssets []qdc.Data) map[string]qdc.Data {
	mapColumnAssetsByColumnName := make(map[string]qdc.Data)
	for _, columnAsset := range columnAssets {
		mapColumnAssetsByColumnName[columnAsset.PhysicalName] = columnAsset
	}
	return mapColumnAssetsByColumnName
}

func GetDescUpdatedSchema(columnAssets []qdc.Data, tableMetadata *bq.TableMetadata) ([]*bq.FieldSchema, bool) {
	var tableSchemas []*bq.FieldSchema
	shouldSchemaUpdated := false
	mapColumnAssetByColumnName := MapColumnAssetByColumnName(columnAssets)
	for _, schemaField := range tableMetadata.Schema {
		newSchemaField := schemaField // copy
		if columnAsset, ok := mapColumnAssetByColumnName[newSchemaField.Name]; ok {
			if newSchemaField.Description == "" && columnAsset.Description != "" {
				descWithPrefix := utils.AddQDICToStringAsPrefix(columnAsset.Description)
				newSchemaField.Description = descWithPrefix
				shouldSchemaUpdated = true
			}
		}
		tableSchemas = append(tableSchemas, newSchemaField)
	}
	return tableSchemas, shouldSchemaUpdated
}
