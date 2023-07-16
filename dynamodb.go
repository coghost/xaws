package xaws

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	TypeN = types.ScalarAttributeTypeN
	TypeS = types.ScalarAttributeTypeS
	TypeB = types.ScalarAttributeTypeB
)

type DynamodbWrapper struct {
	Config aws.Config
	Client *dynamodb.Client
	DdbCtx context.Context

	readCapacity  int
	writeCapacity int

	TableName string

	Timeout int
}

func NewDynamodbWrapper(table string, config aws.Config, readCapacity, writeCapacity int) *DynamodbWrapper {
	return &DynamodbWrapper{
		Config:    config,
		Client:    dynamodb.NewFromConfig(config),
		DdbCtx:    context.TODO(),
		TableName: table,

		readCapacity:  readCapacity,
		writeCapacity: writeCapacity,
	}
}

// TableExists determines whether a DynamoDB table exists.
func (w *DynamodbWrapper) TableExists() (bool, error) {
	exists := true
	_, err := w.Client.DescribeTable(w.DdbCtx, &dynamodb.DescribeTableInput{TableName: aws.String(w.TableName)})
	if err != nil {
		exists = false
	}
	return exists, err
}

// ListTables lists the DynamoDB table names for the current account.
func (w *DynamodbWrapper) ListTables() ([]string, error) {
	var tableNames []string
	tables, err := w.Client.ListTables(w.DdbCtx, &dynamodb.ListTablesInput{})
	if err == nil {
		tableNames = tables.TableNames
	}
	return tableNames, err
}

func (w *DynamodbWrapper) BuildTableInput(pk string, sk string, skType types.ScalarAttributeType) *dynamodb.CreateTableInput {
	ads := []types.AttributeDefinition{{
		AttributeName: aws.String(pk),
		AttributeType: types.ScalarAttributeTypeS,
	}}
	kss := []types.KeySchemaElement{{
		AttributeName: aws.String(pk),
		KeyType:       types.KeyTypeHash,
	}}

	if sk != "" {
		ads = append(ads, types.AttributeDefinition{
			AttributeName: aws.String(sk),
			AttributeType: skType,
		})
		kss = append(kss, types.KeySchemaElement{
			AttributeName: aws.String(sk),
			KeyType:       types.KeyTypeRange,
		})
	}

	ti := &dynamodb.CreateTableInput{
		AttributeDefinitions: ads,
		KeySchema:            kss,
		TableName:            aws.String(w.TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(w.readCapacity)),
			WriteCapacityUnits: aws.Int64(int64(w.writeCapacity)),
		},
	}
	return ti
}

func (w *DynamodbWrapper) CreateTable(tableInput *dynamodb.CreateTableInput) (desc *types.TableDescription, err error) {
	tableInput.TableName = aws.String(w.TableName)
	tb, err := w.Client.CreateTable(
		w.DdbCtx,
		tableInput,
	)
	if err != nil {
		return
	}

	waiter := dynamodb.NewTableExistsWaiter(w.Client)
	err = waiter.Wait(
		w.DdbCtx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(w.TableName),
		},
		5*time.Minute,
	)
	if err != nil {
		return
	}

	return tb.TableDescription, err
}

func (w *DynamodbWrapper) AddItem(data interface{}) error {
	item, err := attributevalue.MarshalMap(data)
	if err != nil {
		panic(err)
	}
	_, err = w.Client.PutItem(w.DdbCtx, &dynamodb.PutItemInput{
		TableName: aws.String(w.TableName), Item: item,
	})
	return err
}

func (w *DynamodbWrapper) AddItemBatch(data []types.WriteRequest) (int, error) {
	var err error
	written := 0
	// DynamoDB allows a maximum batch size of 25 items.
	batchSize := 25
	start := 0
	end := start + batchSize

	for start < len(data) {
		var wrArr []types.WriteRequest
		if end > len(data) {
			end = len(data)
		}

		wrArr = data[start:end]
		_, err = w.Client.BatchWriteItem(
			w.DdbCtx,
			&dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{w.TableName: wrArr},
			},
		)
		if err == nil {
			written += len(wrArr)
		}
		start = end
		end += batchSize
	}

	return written, err
}

func (w *DynamodbWrapper) BuildAttrValueMap(keys []string, values []interface{}) (map[string]types.AttributeValue, error) {
	mapped := make(map[string]types.AttributeValue)
	for i, key := range keys {
		v, err := attributevalue.Marshal(values[i])
		if err != nil {
			return nil, err
		}
		mapped[key] = v
	}
	return mapped, nil
}

func (w *DynamodbWrapper) Retrieve(key map[string]types.AttributeValue, out interface{}) error {
	resp, err := w.Client.GetItem(w.DdbCtx, &dynamodb.GetItemInput{
		Key:       key,
		TableName: aws.String(w.TableName),
	})

	if err != nil {
		return err
	}

	err = attributevalue.UnmarshalMap(resp.Item, out)
	return err
}

func (w *DynamodbWrapper) BuildQueryExpr(name string, key interface{}) (expr expression.Expression, err error) {
	keyEx := expression.Key(name).Equal(expression.Value(key))
	expr, err = expression.NewBuilder().WithKeyCondition(keyEx).Build()
	return expr, err
}

func (w *DynamodbWrapper) Query(expr expression.Expression, out interface{}) (err error) {
	resp, err := w.Client.Query(
		w.DdbCtx,
		&dynamodb.QueryInput{
			TableName:                 aws.String(w.TableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
		},
	)

	if err != nil {
		return
	}

	err = attributevalue.UnmarshalListOfMaps(resp.Items, out)
	return err
}

func (w *DynamodbWrapper) BuildScanExpr() {
}

func (w *DynamodbWrapper) Scan(expr expression.Expression, out interface{}) (err error) {
	resp, err := w.Client.Scan(
		w.DdbCtx,
		&dynamodb.ScanInput{
			TableName:                 aws.String(w.TableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
		},
	)
	if err != nil {
		return
	}

	return attributevalue.UnmarshalListOfMaps(resp.Items, out)
}

func (w *DynamodbWrapper) DeleteRow(key map[string]types.AttributeValue) error {
	_, err := w.Client.DeleteItem(
		w.DdbCtx,
		&dynamodb.DeleteItemInput{
			TableName: aws.String(w.TableName),
			Key:       key,
		},
	)

	return err
}

func (w *DynamodbWrapper) DeleteTable() error {
	_, err := w.Client.DeleteTable(
		w.DdbCtx,
		&dynamodb.DeleteTableInput{
			TableName: aws.String(w.TableName),
		},
	)
	return err
}
