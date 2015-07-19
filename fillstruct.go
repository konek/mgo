package mgo

import (
	"encoding/json"

	"github.com/mitchellh/mapstructure"
)

func FillStruct(m interface{}, v interface{}) error {
	err := mapstructure.WeakDecode(m, v)
	if err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, v)
	if _, ok := err.(*json.UnmarshalTypeError); err == nil || ok == true {
		return nil
	}
	return err
}
