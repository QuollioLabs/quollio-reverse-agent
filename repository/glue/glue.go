package glue

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws/awserr"
)


type GlueClient struct {
	GlueClient *glue.Client
}

func NewGlueClient(roleARN string, profileName string) (GlueClient, error) {
	switch profileName {
	case "":		
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion("ap-northeast-1"),
		)
		if err != nil {
			return GlueClient{}, err
		}
		glueClient := GlueClient{
			GlueClient: returnGlueClient(cfg, roleARN),
		}
		return glueClient, nil
	default:
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion("ap-northeast-1"),
			config.WithSharedConfigProfile(profileName),
		)
		if err != nil {
			return GlueClient{}, err
		}
		glueClient := GlueClient{
			GlueClient: returnGlueClient(cfg, roleARN),
		}
		return glueClient, nil
	}
}

func returnGlueClient(cfg aws.Config, roleARN string) *glue.Client {
	stsSvc := sts.NewFromConfig(cfg)
	creds := stscreds.NewAssumeRoleProvider(stsSvc, roleARN)
	cfg.Credentials = aws.NewCredentialsCache(creds)
	glueClient := glue.NewFromConfig(cfg)
	return glueClient	
}

func (g *GlueClient) GetDatabases(accountID, nextToken string) (*glue.GetDatabasesOutput, error) {
	ctx := context.Background()
	glueDBsInput := glue.GetDatabasesInput{
		CatalogId: &accountID,
		NextToken: &nextToken,
		ResourceShareType: "ALL",
	}
	dbs , err := g.GlueClient.GetDatabases(ctx, &glueDBsInput)
	if err != nil {
		if strings.Contains(err.Error(), "api error InvalidClientTokenId") {
			fmt.Println(err)
			// TODO: add error message
		} else {
			return nil, err
		}
		if awsErr, ok := err.(awserr.Error); ok {
			fmt.Println("awsErr", awsErr)
		}
		return nil, err
	}
	return dbs, nil
}

func (g *GlueClient) UpdateDatabase(dbInput types.DatabaseInput, accountID, databaseName string) (*glue.UpdateDatabaseOutput, error) {
	ctx := context.Background()
	updateDBsInput := glue.UpdateDatabaseInput{
		DatabaseInput: &dbInput,
		CatalogId: &accountID,
		Name: &databaseName,
	}
	output , err := g.GlueClient.UpdateDatabase(ctx, &updateDBsInput)
	if err != nil {
		if strings.Contains(err.Error(), "api error InvalidClientTokenId") {
			fmt.Println(err)
			// TODO: add error message
		} else {
			return nil, err
		}
		if awsErr, ok := err.(awserr.Error); ok {
			fmt.Println("awsErr", awsErr)
		}
		return nil, err
	}
	return output, nil
}

func (g *GlueClient) GetTable(catalogID, dbName string) (*glue.GetTableOutput, error) {
	ctx := context.Background()
	glueTableInput := glue.GetTableInput{
		CatalogId: &catalogID,
		DatabaseName: &dbName,
	}
	table , err := g.GlueClient.GetTable(ctx, &glueTableInput)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (g *GlueClient) GetTables(catalogID, dbName, nextToken string) (*glue.GetTablesOutput, error) {
	ctx := context.Background()
	glueTablesInput := glue.GetTablesInput{
		CatalogId: &catalogID,
		DatabaseName: &dbName,
		NextToken: &nextToken,
	}
	tables , err := g.GlueClient.GetTables(ctx, &glueTablesInput)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (g *GlueClient) UpdateTable(catalogID, dbName string, tableInput types.TableInput) (*glue.UpdateTableOutput, error) {
	ctx := context.Background()
	updateTableInput := glue.UpdateTableInput{
		CatalogId: &catalogID,
		TableInput: &tableInput,
		DatabaseName: &dbName,
	}
	output , err := g.GlueClient.UpdateTable(ctx, &updateTableInput)
	if err != nil {
		return nil, err
	}
	return output, nil
}
