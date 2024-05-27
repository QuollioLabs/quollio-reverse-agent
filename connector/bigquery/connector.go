package bigquery

import (
	"fmt"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/bigquery"
	"quollio-reverse-agent/repository/dataplex"
	"quollio-reverse-agent/repository/qdc"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"
)

type BigQueryConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	DataplexRepo         dataplex.DataplexClient
	BigQueryRepo         bigquery.BigQueryClient
	AssetCreatedBy       string
	OverwriteMode        string
	PrefixForUpdate      string
	Logger               *logger.BuiltinLogger
}

func NewBigqueryConnector(prefixForUpdate, overwriteMode string, logger *logger.BuiltinLogger) (BigQueryConnector, error) {
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
	assetCreatedBy := os.Getenv("QDC_ASSET_CREATED_BY")
	externalAPI := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret, logger)
	connector := BigQueryConnector{
		QDCExternalAPIClient: externalAPI,
		DataplexRepo:         dataplexClient,
		BigQueryRepo:         bigqueryClient,
		AssetCreatedBy:       assetCreatedBy,
		OverwriteMode:        overwriteMode,
		PrefixForUpdate:      prefixForUpdate,
		Logger:               logger,
	}

	return connector, nil
}

func (b *BigQueryConnector) ReflectDatasetDescToBigQuery(schemaAssets []qdc.Data) error {
	for _, schemaAsset := range schemaAssets {
		datasetMetadata, err := b.BigQueryRepo.GetDatasetMetadata(schemaAsset.PhysicalName)
		if err != nil {
			b.Logger.Error("Failed to GetDatasetMetadata. : %s", schemaAsset.PhysicalName)
			return err
		}
		if shouldUpdateBqDataset(b.PrefixForUpdate, b.OverwriteMode, datasetMetadata, schemaAsset) {
			descWithPrefix := utils.AddPrefixToStringIfNotHas(b.PrefixForUpdate, schemaAsset.Description)
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

		columnAssets, err := b.QDCExternalAPIClient.GetChildAssetsByParentAsset(tableAsset)
		if err != nil {
			b.Logger.Error("Failed to GetChildAssetsByParentAsset: %s", tableMetadata.Name)
			return err
		}

		tableSchemas, shouldSchemaUpdated := GetDescUpdatedSchema(b.PrefixForUpdate, b.OverwriteMode, columnAssets, tableMetadata)
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
		if shouldUpdateBqTable(b.PrefixForUpdate, b.OverwriteMode, tableAssetEntry, tableAsset) {
			b.Logger.Debug("The overview of table asset will be updated.: %s", tableAsset.PhysicalName)
			descWithPrefix := utils.AddPrefixToStringIfNotHas(b.PrefixForUpdate, tableAsset.Description)
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
	rootAssets, err := b.QDCExternalAPIClient.GetAllRootAssets("bigquery", b.AssetCreatedBy)
	if err != nil {
		b.Logger.Error("Failed to GetAllBigQueryRootAssets: %s", err.Error())
		return err
	}

	b.Logger.Info("List BigQuery schema assets")
	schemaAssets, err := b.QDCExternalAPIClient.GetAllChildAssetsByID(rootAssets)
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
	tableAssets, err := b.QDCExternalAPIClient.GetAllChildAssetsByID(schemaAssets)
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

func GetDescUpdatedSchema(prefixForUpdate, overwriteMode string, columnAssets []qdc.Data, tableMetadata *bq.TableMetadata) ([]*bq.FieldSchema, bool) {
	var tableSchemas []*bq.FieldSchema
	shouldSchemaUpdated := false
	mapColumnAssetByColumnName := MapColumnAssetByColumnName(columnAssets)
	for _, schemaField := range tableMetadata.Schema {
		newSchemaField := schemaField // copy
		if columnAsset, ok := mapColumnAssetByColumnName[newSchemaField.Name]; ok {
			if shouldUpdateBqColumn(prefixForUpdate, overwriteMode, newSchemaField, columnAsset) {
				descWithPrefix := utils.AddPrefixToStringIfNotHas(prefixForUpdate, columnAsset.Description)
				newSchemaField.Description = descWithPrefix
				shouldSchemaUpdated = true
			}
		}
		tableSchemas = append(tableSchemas, newSchemaField)
	}
	return tableSchemas, shouldSchemaUpdated
}

func shouldUpdateBqDataset(prefixForUpdate, overwriteMode string, datasetMetadata *bq.DatasetMetadata, qdcDataset qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcDataset.Description != "" {
		return true
	}
	if datasetMetadata.Description == "" && qdcDataset.Description != "" {
		return true
	}
	if strings.HasPrefix(datasetMetadata.Description, prefixForUpdate) && qdcDataset.Description != "" {
		return true
	}
	return false
}

func shouldUpdateBqTable(prefixForUpdate, overwriteMode string, tableMetadata *datacatalogpb.Entry, qdcTable qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcTable.Description != "" {
		return true
	}
	if (tableMetadata.BusinessContext == nil || tableMetadata.BusinessContext.EntryOverview.Overview == "") && qdcTable.Description != "" {
		return true
	}

	// MEMO: BusinessContext is markdown. Then, it's possible that `<p>` is unexpectedly inserted into the description.
	if (tableMetadata.BusinessContext == nil || strings.HasPrefix(strings.Replace(tableMetadata.BusinessContext.EntryOverview.Overview, "<p>", "", -1), prefixForUpdate)) && qdcTable.Description != "" {
		return true
	}
	return false
}

func shouldUpdateBqColumn(prefixForUpdate, overwriteMode string, columnMetadata *bq.FieldSchema, qdcColumn qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && qdcColumn.Description != "" {
		return true
	}
	if columnMetadata.Description == "" && qdcColumn.Description != "" {
		return true
	}
	if strings.HasPrefix(columnMetadata.Description, prefixForUpdate) && qdcColumn.Description != "" {
		return true
	}
	return false
}
