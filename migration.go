package mgo

type MigrationFn func(data M, next string)

type Migration struct {
	Version string
	Fn      MigrationFn
}

type Migrations []Migration

func Migrate(collection string, doc M, migrations map[string]Migrations) {
	if _, ok := migrations[collection]; ok == false {
		return
	}
	version, ok := doc["version"]
	if ok == false {
		version = "" // No version = "" version
	}
	for i, m := range migrations[collection] {
		if string(m.Version) == version {
			var next string
			if i < len(migrations[collection])-1 {
				next = migrations[collection][i+1].Version
			} else {
				next = m.Version // if last version, setting next to last
			}
			m.Fn(doc, next)
			if doc["version"] == string(m.Version) { // Version unchanged, either last or error
				return
			}
			Migrate(collection, doc, migrations)
			return
		}
	}
}
