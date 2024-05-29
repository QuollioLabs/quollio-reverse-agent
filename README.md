# quollio-reverse-agent

## 説明

このライブラリは、Quollio Data Intelligence Cloud (QDIC)上のメタデータを取得し、各クラウドサービスのデータカタログに反映させます。  
現在対応しているデータカタログサービスは以下となっております。
- Google BigQuery(Dataplex)
- Amazon Athena(Glue DataCatalog)
- Denodo Virtual DataPort(Denodo DataCatalog)


## 前提条件
このシステムを使用する前に、以下の手順を実行する必要があります。
- Metadata Agentを使用して、データカタログにアセットを登録する。
- 外部API用の、QDIC上で認証に必要なクライアントIDとシークレットを発行する。

## 更新について
### 更新される値
Reverse agentは、各クラウドサービスに存在するリソースの項目値をQDICアセットの値で更新します。  
現在は、下記の項目を対象としています。`<サービス名><リソース名><項目名>`として、記載します。  
### BigQuery
```
BigQuery.Dataset.Description: QDIC.Database.Description  
Dataplex.Table.Overview: QDIC.Table.Description  
BigQuery.Column.Description: QDIC.Column.Description  
```
### Athena
```
Athena.Database.Description: QDIC.Database.Description  
Athena.Table.Overview: QDIC.Table.Description  
Athena.Column.Description: QDIC.Column.Description  
```

### Denodo
```
*`【項目名称】<QDICの論理名>\n【説明】<QDICの説明>`という形式で更新します。  
DenodoVDP.Database.Description: `QDIC.Database.LogicalName+QDIC.Database.Description`  
DenodoVDP.Table.Overview: `QDIC.Table.LogicalName+QDIC.Table.Description`  
DenodoVDP.Column.Description: `QDIC.Column.LogicalName+QDIC.Column.Description`  
DenodoDataCatalog.Database.Description: `QDIC.Database.LogicalName+QDIC.Database.Description`  
DenodoDataCatalog.Table.Overview: `QDIC.Table.LogicalName+QDIC.Table.Description`  
DenodoDataCatalog.Column.Description: `QDIC.Column.LogicalName+QDIC.Column.Description`  
```

### 更新条件
データ更新は、以下2通りの条件のいずれかで行われます。
- 条件1:
  - 更新対象の項目の値がnull、空文字である。
  - 更新対象の項目の値の先頭に、Reverse agent実行時に指定したプレフィックスがついている。
    
- 条件2:
  - 更新対象の項目の値がnull、空文字であるに関わらず、更新する。
条件の選択と項目のプレフィックスは、実行時のパラメータ選択によって行うことができます。


## 実行方法
ローカル環境で実行する際の説明を記載します。

1. リポジトリのルートレベルに`.env`ファイルを作成します。
1. 必要な環境変数をファイルに定義していきます。
1. 設定完了後、`make run`コマンドを実行します。

必要な環境変数は次のとおりです。クラウドサービスごとに実行するため、システム共通の変数と、いずれかのクラウドサービスの変数を設定する必要があります。
### システム共通
```
SYSTEM_NAME=<(Required) システム名。次のうちから一つ選択する。`bigquery`, `athena` or `denodo`>  
COMPANY_ID=<(Required) QDICログインに使用するテナントID>  
QDC_BASE_URL=<(Required) QDIC EXternalAPIのBase URL>  
QDC_CLIENT_ID=<(Required) QDIC EXternalAPIのクライアントID>  
QDC_CLIENT_SECRET=<(Required) QDIC EXternalAPIのクライアントシークレット>  
QDC_ASSET_CREATED_BY=<(Optional) QDICにアセットを登録したユーザー名。入力することで、更新するアセットをフィルタすることができます。>  
OVERWRITE_MODE=<(Optional) OVERWRITE_IF_EMPTY or OVERWRITE_ALL。説明は下部に記載しています。デフォルト値は`OVERWRITE_IF_EMPTY`となります。>  
PREFIX_FOR_UPDATE=<(Optional) 更新時に値につけるPrefix値。`OVERWRITE_MODE`の値に`OVERWRITE_IF_EMPTY`を設定している場合、このPrefixが値についた項目は更新対象となります。デフォルト値は【QDIC】です。>  
LOG_LEVEL=<(Optional)ログレベル。デフォルトは`INFO`で、`DEBUG`に切り替えることで開発用のログを確認できます。>  
```

### BigQuery
```
GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS=<(Required) サービスアカウントのJSON値>
```

### Athena
```
AWS_IAM_ROLE_FOR_GLUE_TABLE=<(Required) IAMロール名>  
ATHENA_ACCOUNT_ID=<(Required) Athenaの存在するアカウントID>  
PROFILE_NAME=<(Optional) ローカル実行する場合に必要となるプロファイル名>  
```

### Denodo
```
DENODO_HOST_NAME=<(Required) VDPホスト名>  
DENODO_CLIENT_ID=<(Required) VDPユーザー名>  
DENODO_CLIENT_SECRET=<(Required) VDPユーザーパスワード>  
DENODO_DEFUALT_DB_NAME=<(Required) VDPデフォルトデータベース>  
DENODO_ODBC_PORT=<(Required) VDP ODBCポート>  
DENODO_REST_API_PORT=<(Required) VDP REST APIポート>  
```

### 補足
OVERWRITE_MODEの値は次の条件に従って設定してください。
- OVERWRITE_IF_EMPTY: 更新条件の条件1で実行する。
- OVERWRITE_ALL: 更新条件の条件2で実行する。

PREFIX_FOR_UPDATEの値は、条件1で実行した場合に使用されます。  
こちらの値が設定されている場合は、値がnullや空文字以外の値でも更新されます。  
デフォルトの値は、`【QDIC】`です。

## 開発
### ユニットテスト

To run unit tests, run the following command.

ユニットテストを実行するには、下記のコマンドを実行してください。

```
$ make test
```


## Description
This library retrieves metadata from the Quollio Data Intelligence Cloud (QDIC) and reflects it in the data catalog of each cloud service.
Currently supported data catalog services are:
- Google BigQuery (Dataplex)
- Amazon Athena (Glue DataCatalog)
- Denodo Virtual DataPort (Denodo DataCatalog)


## Prerequisite
Before using this system, you need to perform the following steps:

- Register assets in the data catalog using the Metadata Agent.
- Issue the client ID and secret required for authentication on QDIC for external APIs.

## About Updates
### Values to be Updated
The reverse agent updates the values of resource items existing in each cloud service with the values of QDIC assets.
Currently, the following items are targeted. They are listed as <ServiceName><ResourceName><ItemName>.

### BigQuery
```
BigQuery.Dataset.Description: QDIC.Database.Description  
Dataplex.Table.Overview: QDIC.Table.Description  
BigQuery.Column.Description: QDIC.Column.Description  
```

### Athena
```
Athena.Database.Description: QDIC.Database.Description  
Athena.Table.Overview: QDIC.Table.Description  
Athena.Column.Description: QDIC.Column.Description  
```

### Denodo
```
*Items are updated in the format `【ItemName】<QDIC LogicalName>\n【Description】<QDIC Description>`  
DenodoVDP.Database.Description: `QDIC.Database.LogicalName+QDIC.Database.Description`  
DenodoVDP.Table.Overview: `QDIC.Table.LogicalName+QDIC.Table.Description`  
DenodoVDP.Column.Description: `QDIC.Column.LogicalName+QDIC.Column.Description`  
DenodoDataCatalog.Database.Description:` QDIC.Database.LogicalName+QDIC.Database.Description`  
DenodoDataCatalog.Table.Overview: `QDIC.Table.LogicalName+QDIC.Table.Description`  
DenodoDataCatalog.Column.Description: `QDIC.Column.LogicalName+QDIC.Column.Description`  
```

### Update Conditions
Data updates are performed under either of the following two conditions:

- Condition 1:
  - The value of the target item to be updated is null or an empty string.
  - The value of the target item to be updated has a prefix specified at the time of reverse agent execution.

- Condition 2:
  - The value of the target item is updated regardless of whether it is null or an empty string.
The selection of conditions and the prefix for items can be specified by parameters at runtime.

## Execution
The instructions for running locally are described below.

- Create a .env file at the root level of the repository.
- Define the necessary environment variables in the file.
- After completing the setup, run the `make run` command.

The required environment variables are as follows. To execute for each cloud service, you need to set common system variables as well as variables for one of the cloud services.

# System common
```
SYSTEM_NAME=<(Required) System name. Choose one from the following: `bigquery`, `athena`, or `denodo`>  
COMPANY_ID=<(Required) Tenant ID used for QDIC login>  
QDC_BASE_URL=<(Required) Base URL for QDIC External API>  
QDC_CLIENT_ID=<(Required) Client ID for QDIC External API>  
QDC_CLIENT_SECRET=<(Required) Client Secret for QDIC External API>  
QDC_ASSET_CREATED_BY=<(Optional) Username of the user who registered the asset in QDIC. By entering this, you can filter the assets to be updated.>  
OVERWRITE_MODE=<(Optional) OVERWRITE_IF_EMPTY or OVERWRITE_ALL. Descriptions are provided below. The default value is `OVERWRITE_IF_EMPTY`>  
PREFIX_FOR_UPDATE=<(Optional) The prefix value to be added to the value during the update. If the value of OVERWRITE_MODE is set to OVERWRITE_IF_EMPTY, items with this prefix value will be targeted for updates. The default value is 【QDIC】.>  
LOG_LEVEL=<(Optional)Log level。`INFO` is set as default value. You can see debug log by switching it to `DEBUG`>  
```

### BigQuery
```
GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS=<(Required) JSON value of the service account>  
```

### Athena
```
AWS_IAM_ROLE_FOR_GLUE_TABLE=<(Required) IAM role name>  
ATHENA_ACCOUNT_ID=<(Required) Account ID where Athena exists>  
PROFILE_NAME=<(Optional) Profile name required for local execution>  
```

### Denodo
```
DENODO_HOST_NAME=<(Required) VDP host name>  
DENODO_CLIENT_ID=<(Required) VDP username>  
DENODO_CLIENT_SECRET=<(Required) VDP user password>  
DENODO_DEFAULT_DB_NAME=<(Required) VDP default database>  
DENODO_ODBC_PORT=<(Required) VDP ODBC port>  
DENODO_REST_API_PORT=<(Required) VDP REST API port>  
```

### Supplementary Information
Please set the value of OVERWRITE_MODE according to the following conditions:  
- OVERWRITE_IF_EMPTY: Executes with condition 1 of the update conditions.  
- OVERWRITE_ALL: Executes with condition 2 of the update conditions.  

The value of PREFIX_FOR_UPDATE is used when executed with condition 1.  
If this value is set, it will be updated even if the value is not null or an empty string.  
The default value is 【QDIC】.  


## Development
### Unit Test
To run unit tests, run the following command
```
$ make test
```
