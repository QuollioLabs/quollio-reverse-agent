package denodo

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quollio-reverse-agent/repository/denodo/model"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
)

type DenodoRepo struct {
	UserPath   string
	BaseURL    string
	HttpClient *http.Client
}

type Database struct {
	DatabaseDescription  string `json:"databaseDescription"`
	DatabaseId           int    `json:"databaseId"`
	DatabaseName         string `json:"databaseName"`
	DescriptionType      string `json:"descriptionType"`
	LastModificationDate struct {
		CalendarType           string `json:"calendarType"`
		FirstDayOfWeek         int    `json:"firstDayOfWeek"`
		Lenient                bool   `json:"lenient"`
		MinimalDaysInFirstWeek int    `json:"minimalDaysInFirstWeek"`
		Time                   string `json:"time"`
		TimeInMillis           int64  `json:"timeInMillis"`
		TimeZone               struct {
			DisplayName string `json:"displayName"`
			Dstsavings  int    `json:"dstsavings"`
			Id          string `json:"id"`
			RawOffset   int    `json:"rawOffset"`
		} `json:"timeZone"`
		WeekDateSupported bool `json:"weekDateSupported"`
		WeekYear          int  `json:"weekYear"`
		WeeksInWeekYear   int  `json:"weeksInWeekYear"`
	} `json:"lastModificationDate"`
	SearchDescription string `json:"searchDescription"`
	ServerId          int    `json:"serverId"`
	SynchDate         struct {
		CalendarType           string `json:"calendarType"`
		FirstDayOfWeek         int    `json:"firstDayOfWeek"`
		Lenient                bool   `json:"lenient"`
		MinimalDaysInFirstWeek int    `json:"minimalDaysInFirstWeek"`
		Time                   string `json:"time"`
		TimeInMillis           int64  `json:"timeInMillis"`
		TimeZone               struct {
			DisplayName string `json:"displayName"`
			Dstsavings  int    `json:"dstsavings"`
			Id          string `json:"id"`
			RawOffset   int    `json:"rawOffset"`
		} `json:"timeZone"`
		WeekDateSupported bool `json:"weekDateSupported"`
		WeekYear          int  `json:"weekYear"`
		WeeksInWeekYear   int  `json:"weeksInWeekYear"`
	} `json:"synchDate"`
}

type PutDatabaseInput struct {
	DatabaseID      int    `json:"databaseId"`
	Description     string `json:"description"`
	DescriptionType string `json:"descriptionType"`
}

type View struct {
	DB                   string  `json:"db"`
	Deleted              bool    `json:"deleted"`
	Description          string  `json:"description"`
	ElementSubType       string  `json:"elementSubtype"`
	ElementType          string  `json:"elementType"`
	Fields               []Field `json:"fields"`
	ID                   int     `json:"id"`
	LastModificationDate string  `json:"lastModificationDate"`
	Name                 string  `json:"name"`
	Path                 string  `json:"path"`
	Value                string  `json:"string"`
}

type FieldProperties struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type Field struct {
	Description     string            `json:"description"`
	FieldProperties []FieldProperties `json:"fieldProperties"`
	Name            string            `json:"name"`
	VdpOrder        int               `json:"vdpOrder"`
}

type LastModificationDate struct {
	CalendarType           string `json:"calendarType"`
	FirstDayOfWeek         int    `json:"firstDayOfWeek"`
	Lenient                bool   `json:"lenient"`
	MinimalDaysInFirstWeek int    `json:"minimalDaysInFirstWeek"`
	Time                   string `json:"time"`
	TimeInMillis           int    `json:"timeInMillis"`
	TimeZone               struct {
		DisplayName string `json:"displayName"`
		Dstsavings  int    `json:"dstsavings"`
		Id          string `json:"id"`
		RawOffset   int    `json:"rawOffset"`
	} `json:"timeZone"`
	WeekDateSupported bool `json:"weekDateSupported"`
	WeekYear          int  `json:"weekYear"`
	WeeksInWeekYear   int  `json:"weeksInWeekYear"`
}

type UpdateLocalViewInput struct {
	DatabaseID      int    `json:"databaseId"`
	Description     string `json:"description"`
	DescriptionType string `json:"descriptionType"`
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

func (d *DenodoRepo) postRequest(url string, data []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
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
		return nil, fmt.Errorf("POST Request failed with status: %v\n", resp.Status)
	}
	return resp, nil
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

func (d *DenodoRepo) GetLocalDatabases() ([]Database, error) {
	url := fmt.Sprintf("%s/public/api/database-management/local/databases", d.BaseURL)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return []Database{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return []Database{}, err
	}

	var database []Database
	if err := json.Unmarshal(b, &database); err != nil {
		return []Database{}, err
	}

	return database, nil
}

func (d *DenodoRepo) UpdateLocalDatabases(input PutDatabaseInput) error {
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

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(b))

	return nil
}

func (d *DenodoRepo) GetLocalViews() ([]View, error) {
	url := fmt.Sprintf("%s/public/api/views", d.BaseURL)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return []View{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return []View{}, err
	}
	fmt.Println(string(b))

	var views []View
	if err := json.Unmarshal(b, &views); err != nil {
		return []View{}, err
	}

	return views, nil
}

func (d *DenodoRepo) GetViewDetails() (model.ViewDetail, error) {
	url := fmt.Sprintf("%s/public/api/view-details?databaseName=c_test_dmd&viewName=quollio_bv_dmt_query_local_history", d.BaseURL)
	res, err := d.SendRequest("GET", url, nil)
	if err != nil {
		return model.ViewDetail{}, err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return model.ViewDetail{}, err
	}

	var viewDetail model.ViewDetail
	if err := json.Unmarshal(b, &viewDetail); err != nil {
		return model.ViewDetail{}, err
	}

	return viewDetail, nil
}

func (d *DenodoRepo) UpdateLocalViewDescription(input UpdateLocalViewInput) error {
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

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(b))

	return nil
}
