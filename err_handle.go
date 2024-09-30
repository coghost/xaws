package xaws

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
