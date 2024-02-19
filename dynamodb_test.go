package xaws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/coghost/xpretty"
	"github.com/k0kubun/pp/v3"
	"github.com/stretchr/testify/suite"
	"github.com/ungerik/go-dry"
)

type DyanmodbWrapperSuite struct {
	suite.Suite
	w *DynamodbWrapper
}

type Movie struct {
	Title string                 `dynamodbav:"title"`
	Year  int                    `dynamodbav:"year"`
	Info  map[string]interface{} `dynamodbav:"info"`
}

func TestDyanmodbWrapper(t *testing.T) {
	suite.Run(t, new(DyanmodbWrapperSuite))
}

func (s *DyanmodbWrapperSuite) SetupSuite() {
	xpretty.Initialize()
	cfgFile := "/tmp/aws.ddb.cfg"
	lines, e := dry.FileGetLines(cfgFile)
	if e != nil {
		xpretty.DummyErrorLog(cfgFile, "is empty or not exist")
		panic(e)
	}
	ak, sk, region, name := lines[0], lines[1], lines[2], lines[3]
	cfg, _ := NewAwsConfig(ak, sk, region)
	s.w = NewDynamodbWrapper(name, cfg, 10, 10)
}

func (s *DyanmodbWrapperSuite) TearDownSuite() {
}

func (s *DyanmodbWrapperSuite) Test_00_exist() {
	b, e := s.w.TableExists()
	s.False(b)
	s.NotNil(e)
}

func (s *DyanmodbWrapperSuite) Test_01_listTables() {
	tbs, e := s.w.ListTables()

	s.Greater(len(tbs), 0)
	s.Nil(e)
}

func (s *DyanmodbWrapperSuite) Test_02_createTable() {
	ti := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("year"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("title"),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("year"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("title"),
			KeyType:       types.KeyTypeRange,
		}},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}
	desc, e := s.w.CreateTable(ti)
	s.Nil(e)
	pp.Println(desc)
}

var tests = []Movie{
	{Title: "Test movie", Year: 2010},
	{Title: "Test movie - 01", Year: 2010},
	{Title: "Test movie", Year: 2011},
	{Title: "Test movie - 01", Year: 2011},
}

func (s *DyanmodbWrapperSuite) Test_03_addRow() {
	for _, tt := range tests {
		err := s.w.PutItem(tt)
		s.Nil(err)
	}
}

func (s *DyanmodbWrapperSuite) Test_04_getRow() {
	for _, tt := range tests {
		var out Movie
		key, e := s.w.BuildAttrValueMap([]string{"year", "title"}, []interface{}{tt.Year, tt.Title})
		s.Nil(e)
		err := s.w.GetItem(key, &out)
		s.Nil(err)
		pp.Println(out)
	}
}

func (s *DyanmodbWrapperSuite) Test_05_query() {
	for _, tt := range tests[:1] {
		var out []Movie
		expr, err := s.w.BuildQueryExpr("year", tt.Year)
		s.Nil(err)
		err = s.w.Query(expr, &out)
		s.Nil(err)
		pp.Println(out)
	}
}

func buildExpr(name string) (expression.Expression, error) {
	filtEx := expression.Name("year").GreaterThan(expression.Value(2010))
	projEx := expression.NamesList(expression.Name("year"), expression.Name("title"))
	expr, err := expression.NewBuilder().WithFilter(filtEx).WithProjection(projEx).Build()
	return expr, err
}

func (s *DyanmodbWrapperSuite) Test_06_scan() {
	for _, tt := range tests[:1] {
		var out []Movie
		expr, err := buildExpr(tt.Title)
		s.Nil(err)
		err = s.w.Scan(expr, &out)
		s.Nil(err)
		pp.Println(out)
	}
}

func (s *DyanmodbWrapperSuite) Test_07_delete() {
	for _, tt := range tests[:1] {
		var out Movie
		key, e := s.w.BuildAttrValueMap([]string{"year", "title"}, []interface{}{tt.Year, tt.Title})
		s.Nil(e)
		err := s.w.GetItem(key, &out)
		s.Nil(err)
		pp.Println(out)
	}
}
