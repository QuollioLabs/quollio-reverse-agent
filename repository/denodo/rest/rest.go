package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quollio-reverse-agent/repository/denodo/rest/models"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
)

type DenodoRepo struct {
	UserPath   string
	BaseURL    string
	HttpClient *http.Client
}

func NewDenodoRepo(clientID, clientSecret, baseURL string) *DenodoRepo {
	src := []byte(fmt.Sprintf("%s:%s", clientID, clientSecret))
	encoded := base64.RawStdEncoding.EncodeToString(src)
	retryClient := retryablehttp.NewClient()
	retryClient.Logger = nil
	retryClient.RetryMax = 10
	repo := DenodoRepo{
		UserPath:   encoded,
		BaseURL:    baseURL,
		HttpClient: retryClient.StandardClient(),
	}
	return &repo
}

func (d *DenodoRepo) SendRequest(reqType, url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest(reqType, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Request failed with status: %s", err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+d.UserPath)

	resp, err := d.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Request failed with status: %v\n", resp.Status)
	}
	return resp, nil
}

func (d *DenodoRepo) GetLocalDatabases() ([]models.Database, error) {
	url := fmt.Sprintf("%s/public/api/database-management/local/databases", d.BaseURL)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return []models.Database{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return []models.Database{}, err
	}

	var database []models.Database
	if err := json.Unmarshal(b, &database); err != nil {
		return []models.Database{}, err
	}

	return database, nil
}

func (d *DenodoRepo) UpdateLocalDatabases(input models.PutDatabaseInput) error {
	url := fmt.Sprintf("%s/public/api/database-management/local/database", d.BaseURL)
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return err
	}
	res, err := d.SendRequest("PUT", url, inputBytes)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		err = WrapError(res)
		return err
	}

	return nil
}

func (d *DenodoRepo) GetLocalViews() ([]models.View, error) {
	url := fmt.Sprintf("%s/public/api/views", d.BaseURL)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return []models.View{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return []models.View{}, err
	}

	var views []models.View
	if err := json.Unmarshal(b, &views); err != nil {
		return []models.View{}, err
	}

	return views, nil
}

func (d *DenodoRepo) GetViewDetails(databaseName, viewName string) (models.ViewDetail, error) {
	url := fmt.Sprintf("%s/public/api/view-details?databaseName=%s&viewName=%s", d.BaseURL, databaseName, viewName)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return models.ViewDetail{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return models.ViewDetail{}, err
	}

	var viewDetail models.ViewDetail
	if err := json.Unmarshal(b, &viewDetail); err != nil {
		return models.ViewDetail{}, err
	}

	return viewDetail, nil
}

func (d *DenodoRepo) GetViewColumns(databaseName, viewName string) ([]models.ViewColumn, error) {
	url := fmt.Sprintf("%s/public/api/views/fields?databaseName=%s&viewName=%s", d.BaseURL, databaseName, viewName)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return []models.ViewColumn{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return []models.ViewColumn{}, err
	}

	var viewColumns []models.ViewColumn
	if err := json.Unmarshal(b, &viewColumns); err != nil {
		return []models.ViewColumn{}, err
	}

	return viewColumns, nil
}

func (d *DenodoRepo) UpdateLocalViewDescription(input models.UpdateLocalViewInput) error {
	url := fmt.Sprintf("%s/public/api/views", d.BaseURL)
	inputByte, err := json.Marshal(input)
	if err != nil {
		return err
	}
	res, err := d.SendRequest("PUT", url, inputByte)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		err = WrapError(res)
		return err
	}

	return nil
}

func (d *DenodoRepo) UpdateLocalViewFieldDescription(input models.UpdateLocalViewFieldInput) error {
	url := fmt.Sprintf("%s/public/api/views/fields", d.BaseURL)
	inputByte, err := json.Marshal(input)
	if err != nil {
		return err
	}
	res, err := d.SendRequest("PUT", url, inputByte)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		err = WrapError(res)
		return err
	}

	return nil
}
