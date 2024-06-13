package qdc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

type QDCExternalAPI struct {
	BaseURL      string
	ClientID     string
	ClientSecret string
	HttpClient   *http.Client
	Logger       *logger.BuiltinLogger
}

type QDCTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

type GetAssetByTypeResponse struct {
	Data   []Data `json:"data"`
	LastID string `json:"last_id"`
}

type GetAssetByIDsResponse struct {
	Data []Data `json:"data"`
}

type Data struct {
	Path            []Path       `json:"path"`
	RuleTagIds      []RuleTagIds `json:"rule_tag_ids"`
	ManualTagIds    []RuleTagIds `json:"manual_tag_ids"`
	ID              string       `json:"id"`
	SubID           string       `json:"sub_id"`
	ObjectType      string       `json:"object_type"`
	CreatedBy       string       `json:"created_by"`
	UpdatedBy       []string     `json:"updated_by"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
	IsArchived      bool         `json:"is_archived"`
	IsCsvImported   bool         `json:"is_csv_imported"`
	IsLost          bool         `json:"is_lost"`
	ServiceName     string       `json:"service_name"`
	PhysicalName    string       `json:"physical_name"`
	LogicalName     string       `json:"logical_name"`
	Description     string       `json:"description"`
	CommentOnDDL    string       `json:"comment_on_ddl"`
	DataType        string       `json:"data_type"`
	OrdinalPosition int          `json:"ordinal_position"`
	ChildAssetIds   []string     `json:"child_asset_ids"`
}

type Path struct {
	PathLayer  string `json:"path_layer"`
	ID         string `json:"id"`
	ObjectType string `json:"object_type"`
	Name       string `json:"name"`
}

type RuleTagIds struct {
	TagGroupId  string `json:"tag_group_id"`
	ParentTagId string `json:"parent_tag_id"`
	ChildTagId  string `json:"child_tag_id"`
}

func NewQDCExternalAPI(baseURL, clientID, clientSecret string, logger *logger.BuiltinLogger) QDCExternalAPI {
	httpClient := retryablehttp.NewClient()
	httpClient.RetryMax = 10
	httpClient.Logger = nil
	externalAPI := QDCExternalAPI{
		BaseURL:      baseURL,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		HttpClient:   httpClient.StandardClient(),
		Logger:       logger,
	}
	return externalAPI
}

func (q *QDCExternalAPI) postRequest(url string, payload *strings.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return &http.Response{}, err
	}
	token, err := q.GetAccessToken()
	if err != nil {
		return &http.Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := q.HttpClient.Do(req)
	if err != nil {
		return &http.Response{}, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp, nil
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		fmt.Printf("QDC API returns %v. then now retrying", resp.StatusCode)
	default:
		return nil, fmt.Errorf("Request failed with status: %v\n", resp)
	}
	return resp, nil
}

func (q *QDCExternalAPI) GetAccessToken() (string, error) {
	url := fmt.Sprintf("%s/oauth2/token", q.BaseURL)
	form := neturl.Values{}
	form.Add("grant_type", "client_credentials")
	form.Add("client_id", q.ClientID)
	form.Add("scope", "api.quollio.com/beta:admin")
	payload := strings.NewReader(form.Encode())

	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(q.ClientID, q.ClientSecret)

	resp, err := q.HttpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		res, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		var tokenResponse QDCTokenResponse
		if err := json.Unmarshal(res, &tokenResponse); err != nil {
			return "", err
		}
		return tokenResponse.AccessToken, nil
	default:
		return "", err
	}
}

func (q *QDCExternalAPI) GetAssetByIDs(assetIDs []string) (GetAssetByIDsResponse, error) {
	url := fmt.Sprintf("%s/v2/assets/ids", q.BaseURL)
	data := map[string][]string{
		"ids": assetIDs,
	}
	b, err := json.Marshal(data)
	if err != nil {
		return GetAssetByIDsResponse{}, err
	}
	payload := strings.NewReader(string(b))
	resp, err := q.postRequest(url, payload)
	if err != nil {
		return GetAssetByIDsResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GetAssetByIDsResponse{}, err
	}
	var getAssetByIDsResponse GetAssetByIDsResponse
	err = json.Unmarshal([]byte(body), &getAssetByIDsResponse)
	if err != nil {
		return GetAssetByIDsResponse{}, err
	}
	return getAssetByIDsResponse, nil
}

func (q *QDCExternalAPI) GetAssetByType(assetType, lastID string) (GetAssetByTypeResponse, error) {
	url := fmt.Sprintf("%s/v2/assets/type", q.BaseURL)
	data := map[string]string{
		"last_id":     lastID,
		"object_type": assetType,
	}
	b, err := json.Marshal(data)
	if err != nil {
		return GetAssetByTypeResponse{}, err
	}

	payload := strings.NewReader(string(b))
	resp, err := q.postRequest(url, payload)
	if err != nil {
		return GetAssetByTypeResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GetAssetByTypeResponse{}, err
	}
	var getAssetByTypeResponse GetAssetByTypeResponse
	err = json.Unmarshal([]byte(body), &getAssetByTypeResponse)
	if err != nil {
		return GetAssetByTypeResponse{}, err
	}
	return getAssetByTypeResponse, nil
}

func (q *QDCExternalAPI) GetAllRootAssets(serviceName, createdBy string) ([]Data, error) {
	var rootAssets []Data

	var lastAssetID string
	for {
		assetResponse, err := q.GetAssetByType("schema", lastAssetID)
		if err != nil {
			return nil, fmt.Errorf("Failed to GetAssetByType. lastAssetID: %s", lastAssetID)
		}
		for _, assetData := range assetResponse.Data {
			switch assetData.ServiceName {
			case serviceName:
				switch createdBy {
				case "":
					rootAssets = append(rootAssets, assetData)
				default:
					if createdBy == assetData.CreatedBy {
						q.Logger.Debug("Get assets created by : %s", createdBy)
						rootAssets = append(rootAssets, assetData)
					}
				}
			default:
				continue
			}
		}
		switch assetResponse.LastID {
		case "":
			return rootAssets, nil
		default:
			q.Logger.Debug("GetAllRootAssets will continue. lastAssetID: %s", lastAssetID)
			lastAssetID = assetResponse.LastID
		}
	}
}

func (q *QDCExternalAPI) GetAllChildAssetsByID(parentAssets []Data) ([]Data, error) {
	var childAssets []Data

	for _, parentAsset := range parentAssets {
		childAssetIdChunks := utils.SplitArrayToChunks(parentAsset.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
		for _, childAssetIdChunk := range childAssetIdChunks {
			assets, err := q.GetAssetByIDs(childAssetIdChunk)
			if err != nil {
				return nil, err
			}
			childAssets = append(childAssets, assets.Data...)
			q.Logger.Debug("Fetching ChildAssets by parent id %s", parentAsset.ID)
		}
	}
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		q.Logger.Debug("The number of child assets is %v", len(childAssets))
		var childAssetIds []string
		for _, childAsset := range childAssets {
			childAssetIds = append(childAssetIds, childAsset.ID)
		}
		q.Logger.Debug("The child asset ids are %v", childAssetIds)
	}
	return childAssets, nil
}

func (q *QDCExternalAPI) GetChildAssetsByParentAsset(assets Data) ([]Data, error) {
	var childAssets []Data

	childAssetIdChunks := utils.SplitArrayToChunks(assets.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
	for _, childAssetIdChunk := range childAssetIdChunks {
		assets, err := q.GetAssetByIDs(childAssetIdChunk)
		if err != nil {
			return nil, err
		}
		childAssets = append(childAssets, assets.Data...)
	}
	q.Logger.Debug("The number of child asset chunks is %v", len(childAssets))
	return childAssets, nil
}

func GetSpecifiedAssetFromPath(asset Data, pathLayer string) Path {
	path := asset.Path
	for _, p := range path {
		if p.PathLayer == pathLayer {
			return p
		}
	}
	return Path{}
}

func IsAssetContainsValueAsDescription(asset Data) bool {
	return asset.Description != ""
}
