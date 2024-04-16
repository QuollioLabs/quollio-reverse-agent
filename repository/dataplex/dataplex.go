package dataplex

import (
	"context"

	datacatalog "cloud.google.com/go/datacatalog/apiv1"
	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type DataplexClient struct {
	CatalogClient *datacatalog.Client
}

func NewDataplexClient(serviceAccountCredentialJson string) (DataplexClient, error) {
	ctx := context.Background()

	creds, err := google.CredentialsFromJSON(ctx, []byte(serviceAccountCredentialJson), datacatalog.DefaultAuthScopes()...)
	if err != nil {
		return DataplexClient{}, err
	}
	// https://pkg.go.dev/cloud.google.com/go#hdr-Authentication_and_Authorization
	c, err := datacatalog.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return DataplexClient{}, err
	}
	client := DataplexClient{
		CatalogClient: c,
	}

	return client, nil
}

func (d *DataplexClient) ModifyEntryOverview(entryName, entryOverview string) (*datacatalogpb.EntryOverview, error) {
	ctx := context.Background()
	req := &datacatalogpb.ModifyEntryOverviewRequest{
		Name: entryName,
		EntryOverview: &datacatalogpb.EntryOverview{
			Overview: entryOverview,
		},
	}

	resp, err := d.CatalogClient.ModifyEntryOverview(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (d *DataplexClient) LookupEntry(assetFQN, projectName, location string) (*datacatalogpb.Entry, error) {
	ctx := context.Background()
	fqn := &datacatalogpb.LookupEntryRequest_FullyQualifiedName{
		FullyQualifiedName: assetFQN,
	}
	lookupEntryRequest := datacatalogpb.LookupEntryRequest{
		TargetName: fqn,
		Project:    projectName,
		Location:   location,
	}
	res, err := d.CatalogClient.LookupEntry(ctx, &lookupEntryRequest)
	if err != nil {
		return &datacatalogpb.Entry{}, err
	}

	return res, nil
}
