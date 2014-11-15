package JsonMessage

import (
	"strconv"
)

type JsonMessage struct {
	ApplicationId string            `json:"applicationid"` //
	Api           map[string]string `json:"api"`           //
	TimeStamp     int64             `json:"timeStamp"`     //
	Url           string            `json:"url"`           //
	Method        string            `json:"method"`        //
	Host          string            `json:"host"`          //
}

func IntToString(input_num int64) string {
	return strconv.FormatInt(input_num, 10)
}
