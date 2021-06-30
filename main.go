package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type GenericDocument []map[string]interface{}

func main() {

	args := os.Args[1:]
	argsCount := len(args)
	if argsCount < 2 {
		fmt.Printf("Usage:\n\ngo run main.go <input file> <table name> [region]\n\n")
		return
	}

	inputFile := args[0]
	tableName := args[1]
	awsRegion := "us-east-1"
	if argsCount > 2 {
		awsRegion = args[2]
	}

	data, err := ioutil.ReadFile(inputFile)
	if err != nil {
		fmt.Printf("Error while reading file %v", inputFile)
		return
	}

	documents := GenericDocument{}
	err = json.Unmarshal([]byte(data), &documents)
	if err != nil {
		fmt.Printf("Error while unmarshalling json, %v", err)
	}
	fmt.Printf("main: -> file was unmarshalled correctly with %v items, looping..\n", len(documents))

	os.Setenv("AWS_REGION", awsRegion)
	var item interface{}
	client := dynamodb.New(session.New())
	for i, document := range documents {

		fmt.Printf("main: -----------------------------\n")
		fmt.Printf("main: -> current index [%v]\n", i)
		item, err = hydrateItem(1, document, false)
		if err != nil {
			fmt.Printf("Error while hydrating item: %v", err)
			return
		}

		_, err := client.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item.(map[string]*dynamodb.AttributeValue),
		})

		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

	}
	fmt.Printf("main: -> result: \n%v\n", item)

}

func hydrateItem(depth int, document interface{}, isArray bool) (interface{}, error) {

	fmt.Printf("hydrateItem:%v: called\n", depth)
	fmt.Printf("hydrateItem:%v: -> isArray: %v\n", depth, isArray)
	item := map[string]*dynamodb.AttributeValue{}
	if isArray {

		items := []*dynamodb.AttributeValue{}
		iterable := document.([]interface{})
		fmt.Printf("hydrateItem:%v: -> looping through %v subItems..\n", depth, len(iterable))

		for i, v := range iterable {

			fmt.Printf("hydrateItem:%v: -----------------------------\n", depth)
			fmt.Printf("hydrateItem:%v: --> current index [%v]\n", depth, i)
			fmt.Printf("hydrateItem:%v: --> current value type: '%T'\n", depth, v)

			switch fmt.Sprintf("%T", v) {
			case "string":
				newItem := &dynamodb.AttributeValue{
					S: aws.String(v.(string)),
				}
				items = append(items, newItem)
			default:
				newItem, err := hydrateItem(depth+1, v, false)
				if err != nil {
					return "", err
				}
				fmt.Printf("hydrateItem:%v: --> appending got item to list of items..\n", depth)
				items = append(
					items,
					&dynamodb.AttributeValue{
						M: newItem.(map[string]*dynamodb.AttributeValue),
					},
				)

			}

		}
		return items, nil
	}

	jsonMap := document.(map[string]interface{})
	fmt.Printf("hydrateItem:%v: -> looping through %v key:value pairs..\n", depth, len(jsonMap))
	for key, val := range jsonMap {
		fmt.Printf("hydrateItem:%v: --> current key: '%v'\n", depth, key)
		fmt.Printf("hydrateItem:%v: --> current value type: '%T'\n", depth, val)
		switch fmt.Sprintf("%T", val) {
		case "string":
			item[key] = &dynamodb.AttributeValue{
				S: aws.String(val.(string)),
			}
		case "float64":
			item[key] = &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(int(val.(float64)))),
			}
		case "[]interface {}":

			v, err := hydrateItem(depth+1, val, true)
			if err != nil {
				return "", err
			}
			item[key] = &dynamodb.AttributeValue{
				L: v.([]*dynamodb.AttributeValue),
			}

		default:
			return map[string]*dynamodb.AttributeValue{},
				fmt.Errorf(
					"unknown value type %T for key %v",
					val,
					key,
				)
		}
	}

	return item, nil

}
