package xaws

import "fmt"

func ExampleNewDynamodbWrapper() {
	cfg, err := NewAwsConfig("ak", "sk", "region")
	fmt.Println(err == nil)
	cap := 10
	w := NewDynamodbWrapper("", cfg, cap, cap)
	fmt.Println(w == nil)
	// Output:
	// true
	// false
}

func ExampleDynamodbWrapper_TableExists() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	cap := 10
	w := NewDynamodbWrapper("", cfg, cap, cap)
	b, e := w.TableExists()
	fmt.Println(b)
	fmt.Println(e == nil)
	// Output:
	// false
	// false
}

func ExampleDynamodbWrapper_AddItem() {
	cfg, _ := NewAwsConfig("ak", "sk", "region")
	cap := 10
	w := NewDynamodbWrapper("", cfg, cap, cap)
	var data interface{}
	e := w.AddItem(data)
	fmt.Println(e == nil)
	// Output:
	// false
}
