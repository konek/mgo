package mgo

import (
	"encoding/json"
)

func FillStruct(m interface{}, v interface{}) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, v)
	return nil
}
