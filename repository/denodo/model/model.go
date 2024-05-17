package model

type FieldCapabilities struct {
	FieldName    string   `json:"fieldName"`
	Operators    []string `json:"operators"`
	Multiplicity string   `json:"multiplicity"`
}

type Field struct {
	Role              interface{}       `json:"role"`
	Name              string            `json:"name"`
	TypeName          string            `json:"typeName"`
	Description       string            `json:"description"`
	Operation         interface{}       `json:"operation"`
	Type              int               `json:"type"`
	Mandatory         bool              `json:"mandatory"`
	FieldCapabilities FieldCapabilities `json:"fieldCapabilities"`
	Input             bool              `json:"input"`
	Output            bool              `json:"output"`
	MatchName         int               `json:"matchName"`
	MatchDesc         int               `json:"matchDesc"`
}

type SimpleCondition struct {
	Db         interface{} `json:"db"`
	View       interface{} `json:"view"`
	Field      string      `json:"field"`
	Operator   string      `json:"operator"`
	Value      string      `json:"value"`
	MultiValue []string    `json:"multiValue"`
	Expression interface{} `json:"expression"`
}

type OutputField struct {
	Field                 string        `json:"field"`
	Alias                 interface{}   `json:"alias"`
	Expression            interface{}   `json:"expression"`
	Aggregated            bool          `json:"aggregated"`
	Visible               bool          `json:"visible"`
	ExpandFields          []interface{} `json:"expandFields"`
	Role                  interface{}   `json:"role"`
	DataExpandedFieldFrom interface{}   `json:"dataExpandedFieldFrom"`
	DataExpandedField     interface{}   `json:"dataExpandedField"`
	Db                    interface{}   `json:"db"`
	Operation             interface{}   `json:"operation"`
	Multiple              bool          `json:"multiple"`
	RolesToExpand         []interface{} `json:"rolesToExpand"`
}

type SearchForm struct {
	Database         string            `json:"database"`
	View             string            `json:"view"`
	Type             string            `json:"type"`
	SimpleConditions []SimpleCondition `json:"simpleConditions"`
	GroupByEnabled   bool              `json:"groupByEnabled"`
	GroupByFields    []interface{}     `json:"groupByFields"`
	OrderByFields    []interface{}     `json:"orderByFields"`
	OutputFields     []OutputField     `json:"outputFields"`
	Id               interface{}       `json:"id"`
	Name             interface{}       `json:"name"`
	Description      interface{}       `json:"description"`
	SharedQuery      bool              `json:"sharedQuery"`
	Path             interface{}       `json:"path"`
	VqlShellQuery    interface{}       `json:"vqlShellQuery"`
	AllFields        bool              `json:"allFields"`
}

type Schema struct {
	Name            string      `json:"name"`
	LogicalName     interface{} `json:"logicalName"`
	Type            string      `json:"type"`
	Description     string      `json:"description"`
	DescriptionType interface{} `json:"descriptionType"`
	InLocal         bool        `json:"inLocal"`
	Tags            interface{} `json:"tags"`
	PrimaryKey      bool        `json:"primaryKey"`
	Nullable        bool        `json:"nullable"`
	SourceType      string      `json:"sourceType"`
	TypeSize        int         `json:"typeSize"`
	TypeDecimal     int         `json:"typeDecimal"`
}

type Association struct {
	Associations       []interface{} `json:"associations"`
	AssociationsToOne  []interface{} `json:"associationsToOne"`
	AssociationsToMany []interface{} `json:"associationsToMany"`
}

type ConnectionUris struct {
	JdbcConnectionUrl      string      `json:"jdbc-connection-url"`
	OdbcConnectionString32 string      `json:"odbc-connection-string-32bits"`
	OdbcConnectionString64 string      `json:"odbc-connection-string-64bits"`
	RestConnectionUrl      string      `json:"rest-connection-url"`
	OdataConnectionUrl     string      `json:"odata-connection-url"`
	IntroTextConnect       interface{} `json:"intro-text-connect"`
	GraphQlConnectionUrl   string      `json:"graphQl-connection-url"`
}

type PropertyInfo struct {
	SummaryPropertyMap    map[string]interface{} `json:"summaryPropertyMap"`
	GeneralTabPropertyMap map[string]interface{} `json:"generalTabPropertyMap"`
	CustomTabPropertyMap  map[string]interface{} `json:"customTabPropertyMap"`
}

type ViewStatisticsInfo struct {
	DatabaseName        interface{}   `json:"databaseName"`
	Name                interface{}   `json:"name"`
	RowsNumber          interface{}   `json:"rowsNumber"`
	LastUpdated         interface{}   `json:"lastUpdated"`
	FieldStatisticsList []interface{} `json:"fieldStatisticsList"`
}

type ViewDetail struct {
	Id                 int                `json:"id"`
	Name               string             `json:"name"`
	DatabaseName       string             `json:"databaseName"`
	Schema             []Schema           `json:"schema"`
	TotalFields        int                `json:"totalFields"`
	InIndex            bool               `json:"inIndex"`
	ReadPermission     bool               `json:"readPermission"`
	InLocal            bool               `json:"inLocal"`
	InVDP              bool               `json:"inVDP"`
	Tags               []interface{}      `json:"tags"`
	Categories         []interface{}      `json:"categories"`
	Endorsements       interface{}        `json:"endorsements"`
	Warnings           interface{}        `json:"warnings"`
	Deprecations       interface{}        `json:"deprecations"`
	Description        string             `json:"description"`
	DescriptionType    string             `json:"descriptionType"`
	ShowStatistics     bool               `json:"showStatistics"`
	Statistics         []interface{}      `json:"statistics"`
	Association        Association        `json:"association"`
	SearchForm         SearchForm         `json:"searchForm"`
	Field              Field              `json:"field"`
	PropertyInfo       PropertyInfo       `json:"propertyInfo"`
	ViewStatisticsInfo ViewStatisticsInfo `json:"viewStatisticsInfo"`
	SavedQuery         interface{}        `json:"savedQuery"`
	ConnectionUris     ConnectionUris     `json:"connectionUris"`
	HasRequests        bool               `json:"hasRequests"`
}

type ViewColumn struct {
	Name            string      `json:"name"`
	LogicalName     interface{} `json:"logicalName"`
	Type            string      `json:"type"`
	Description     string      `json:"description"`
	DescriptionType interface{} `json:"descriptionType"`
	InLocal         bool        `json:"inLocal"`
	Tags            interface{} `json:"tags"`
	PrimaryKey      bool        `json:"primaryKey"`
	Nullable        bool        `json:"nullable"`
	SourceType      string      `json:"sourceType"`
	TypeSize        int         `json:"typeSize"`
	TypeDecimal     int         `json:"typeDecimal"`
}
