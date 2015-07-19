package mgo

import (
	"fmt"
)

type MigrationFn func(data M, next int)

type MigrationDepFn func(id interface{}, db *Database) error

type Migration struct {
	Version int
	Fn      MigrationFn
	Dep     MigrationDepFn
}

type Migrations []Migration

func Migrate(collection string, doc M, migrations map[string]Migrations, db *Database) {
	if _, ok := migrations[collection]; ok == false {
		return
	}
	version, ok := doc["version"]
	if ok == false {
		version = "" // No version = "" version
	}
	for i, m := range migrations[collection] {
		if string(m.Version) == version {
			var next int
			if i < len(migrations[collection])-1 {
				next = migrations[collection][i+1].Version
			} else {
				next = m.Version // if last version, setting next to last
			}
			m.Fn(doc, next)
			if m.Dep != nil {
				err := m.Dep(doc["_id"], db)
				if err != nil {
					fmt.Println("Error while migrating %s/%s: %s\n", collection, doc["_id"], err)
					return // Don't carry on with errors
				}
			}
			if doc["version"] == string(m.Version) { // Version unchanged, either last or error
				return
			}
			Migrate(collection, doc, migrations, db)
			return
		}
	}
}
