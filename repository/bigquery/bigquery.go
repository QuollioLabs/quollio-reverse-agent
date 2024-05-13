package bigquery

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type BigQueryClient struct {
	BQClient *bigquery.Client
}

func NewBigQueryClient(serviceAccountCredentialJson string) (BigQueryClient, error) {
	ctx := context.Background()
	creds, err := google.CredentialsFromJSON(ctx, []byte(serviceAccountCredentialJson), "https://www.googleapis.com/auth/bigquery")
	if err != nil {
		return BigQueryClient{}, err
	}
	// https://pkg.go.dev/cloud.google.com/go#hdr-Authentication_and_Authorization
	var secretContent map[string]string
	err = json.Unmarshal([]byte(serviceAccountCredentialJson), &secretContent)
	if err != nil {
		return BigQueryClient{}, nil
	}
	c, err := bigquery.NewClient(ctx, secretContent["project_id"], option.WithCredentials(creds))
	if err != nil {
		return BigQueryClient{}, err
	}
	client := BigQueryClient{
		BQClient: c,
	}

	return client, nil
}

func (b *BigQueryClient) GetDatasetMetadata(datasetID string) (*bigquery.DatasetMetadata, error) {
	ctx := context.Background()
	dataset := b.BQClient.Dataset(datasetID)
	datasetMetadata, err := dataset.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return datasetMetadata, nil
}

func (b *BigQueryClient) UpdateDatasetDescription(datasetID, description string) (*bigquery.DatasetMetadata, error) {
	ctx := context.Background()
	dataset := b.BQClient.Dataset(datasetID)
	datasetMetadata, err := dataset.Update(ctx, bigquery.DatasetMetadataToUpdate{
		Description: description,
	}, "")
	if err != nil {
		return nil, err
	}
	return datasetMetadata, nil
}

func (b *BigQueryClient) GetTableMetadata(datasetID, tableName string) (*bigquery.TableMetadata, error) {
	ctx := context.Background()
	table := b.BQClient.Dataset(datasetID).Table(tableName)
	tableMetadata, err := table.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return tableMetadata, nil
}

func (b *BigQueryClient) UpdateTableMetadata(datasetID, tableName string, metadata bigquery.TableMetadataToUpdate) (*bigquery.TableMetadata, error) {
	ctx := context.Background()
	table := b.BQClient.Dataset(datasetID).Table(tableName)
	tableMetadata, err := table.Update(ctx, metadata, "")
	if err != nil {
		return nil, err
	}
	return tableMetadata, nil
}
