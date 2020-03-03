package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// KudosTableNamePrefix pattern for our ddb table
const KudosTableNamePrefix = "ecskudos-%s-%s"

// Kudo is a struct representing a users kudo
type Kudo struct {
	User             string
	Time             time.Time `dynamodbav:",unixtime"`
	ContributionType string
	ContributionURL  string
	ContributionName string
}

// KudosService represents Kudos for folks
type KudosService struct {
	dynamo    *dynamodb.DynamoDB
	tableName string
}

// NewKudosService returns a service for CRUD operations on kudos.
func NewKudosService(sess *session.Session) *KudosService {
	return &KudosService{
		dynamo:    dynamodb.New(sess),
		tableName: fmt.Sprintf(KudosTableNamePrefix, "test", "kudos"),
	}
}

// CreateKudo saves a kudo in the DB
func (s *KudosService) CreateKudo(request *Kudo) error {
	serialized, err := dynamodbattribute.MarshalMap(request)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      serialized,
		TableName: aws.String(s.tableName),
	}

	_, err = s.dynamo.PutItem(input)
	if err != nil {
		return err
	}

	return nil
}

// GetKudos fetches the kudos for a particular user
func (s *KudosService) GetKudos(user string) ([]*Kudo, error) {
	var queryInput = &dynamodb.QueryInput{
		TableName: aws.String(s.tableName),
		KeyConditions: map[string]*dynamodb.Condition{
			"User": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(user),
					},
				},
			},
		},
	}

	var resp, err = s.dynamo.Query(queryInput)
	if err != nil {
		return nil, err
	}

	var kudos []*Kudo
	for _, item := range resp.Items {
		var userKudo Kudo

		err = dynamodbattribute.UnmarshalMap(item, &userKudo)
		if err != nil {
			return nil, err
		}

		kudos = append(kudos, &userKudo)
	}
	return kudos, nil
}
