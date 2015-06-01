package mgo

import (
	"container/list"
	"errors"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// DbQueueFn ...
type DbQueueFn func(*Database, chan error)

// DbQueueFn2 ...
type DbQueueFn2 func(*Database)

// Database ...
type Database struct {
	*mgo.Database
}

// Ref ...
type Ref struct {
	Collection string
	ID         bson.ObjectId
}

// MakeRef
func MakeRef(c string, id bson.ObjectId) Ref {
	return Ref{
		Collection: c,
		ID:         id,
	}
}

// M ...
type M map[string]interface{}

// UpdateReq ...
type UpdateReq struct {
	M
	Prefix string
}

// Do ...
func (u *UpdateReq) Do(collection string, id interface{}, q *DbQueue) error {
	return q.UpdateID(collection, id, u.M)
}

func (u UpdateReq) Set(field string, data interface{}) {
	if u.M == nil {
		u.M = M{}
	}
	if set, ok := u.M["$set"].(M); ok == true {
		set[u.Prefix+field] = data
	} else {
		u.M["$set"] = M{
			u.Prefix + field: data,
		}
	}
}

func (u UpdateReq) Add(field string, data interface{}) {
	if u.M == nil {
		u.M = M{}
	}
	if add, ok := u.M["$addToSet"].(M); ok == true {
		add[u.Prefix+field] = data
	} else {
		u.M["$addToSet"] = M{
			u.Prefix + field: data,
		}
	}
}

func (u UpdateReq) Remove(field string, data interface{}) {
	if u.M == nil {
		u.M = M{}
	}
	if remove, ok := u.M["$pull"].(M); ok == true {
		remove[u.Prefix+field] = data
	} else {
		u.M["$pull"] = M{
			u.Prefix + field: data,
		}
	}
}

func (u UpdateReq) SetUpdated() {
	u.Set("time.updated", time.Now())
}

func (u UpdateReq) SetEnded() {
	u.Set("time.ended", time.Now())
}

// ENotFound ...
type ENotFound struct{}

// Error ...
func (e ENotFound) Error() string {
	return "not found"
}

// DbQueue ...
type DbQueue struct {
	Migrations map[string]Migrations
	queue      chan DbQueueFn2
	dbs        *list.List
}

// NewDbQueue ...
func NewDbQueue(size int) *DbQueue {
	return &DbQueue{
		queue: make(chan DbQueueFn2, size),
		dbs:   list.New(),
	}
}

// Push ...
func (q *DbQueue) Push(cb DbQueueFn) error {
	var err error

	errc := make(chan error)
	q.queue <- func(db *Database) {
		cb(db, errc)
	}
	err = <-errc
	return err
}

// Count ...
func (q *DbQueue) Count(collection string, query interface{}) (int, error) {
	var n int
	err := q.Push(func(db *Database, ec chan error) {
		var e error
		q := db.C(collection).Find(query)
		n, e = q.Count()
		ec <- e
	})
	return n, err
}

// FindOne ...
func (q *DbQueue) FindOne(collection string, ret interface{}, query interface{}) error {
	err := q.Push(func(db *Database, ec chan error) {
		var tmp M
		query := db.C(collection).Find(query)
		n, e := query.Count()
		if e != nil {
			ec <- e
			return
		}
		if n == 0 {
			ec <- ENotFound{}
			return
		}
		e = query.One(&tmp)
		if e != nil {
			ec <- e
			return
		}
		Migrate(collection, tmp, q.Migrations)
		ec <- FillStruct(tmp, ret)
	})
	return err
}

// Find ...
func (q *DbQueue) Find(collection string, ret interface{}, query interface{}) error {
	err := q.Push(func(db *Database, ec chan error) {
		var tmp []M
		query := db.C(collection).Find(query)
		n, e := query.Count()
		if e != nil {
			ec <- e
			return
		}
		if n == 0 {
			ec <- ENotFound{}
			return
		}
		e = query.All(&tmp)
		if e != nil {
			ec <- e
			return
		}
		for _, t := range tmp {
			Migrate(collection, t, q.Migrations)
		}
		ec <- FillStruct(tmp, ret)
	})
	return err
}

// Insert ...
func (q *DbQueue) Insert(collection string, data interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).Insert(data)
	})
}

// UpdateID ...
func (q *DbQueue) UpdateID(collection string, id interface{}, data interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).UpdateId(id, data)
	})
}

// UpdateSet ...
func (q *DbQueue) UpdateSet(collection string, id interface{}, field string, data interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).UpdateId(id, bson.M{
			"$set": bson.M{
				field: data,
			},
		})
	})
}

// UpdateAdd ...
func (q *DbQueue) UpdateAdd(collection string, id interface{}, field string, data interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).UpdateId(id, bson.M{
			"$addToSet": bson.M{
				field: data,
			},
		})
	})
}

// UpdateRemove ...
func (q *DbQueue) UpdateRemove(collection string, id interface{}, field string, data interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).UpdateId(id, bson.M{
			"$pull": bson.M{
				field: data,
			},
		})
	})
}

// Update ...
func (q *DbQueue) Update(collection string, data interface{}, query interface{}) error {
	return q.Push(func(db *Database, ec chan error) {
		ec <- db.C(collection).Update(query, data)
	})
}

// RunDb ...
func (q *DbQueue) RunDb(db *Database) {
	for {
		fn := <-q.queue
		fn(db)
	}
}

// Run ...
func (q *DbQueue) Run() {
	for e := q.dbs.Front(); e != nil; e = e.Next() {
		db, ok := e.Value.(*Database)
		if ok == false {
			continue
		}
		go q.RunDb(db)
	}
}

// AddConnection ...
func (q *DbQueue) AddConnection(url string, name string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return err
	}
	db := session.DB(name)
	q.dbs.PushBack(&Database{db})
	return nil
}

// Ref ...
func (q *DbQueue) Ref(collection string, id interface{}) mgo.DBRef {
	return mgo.DBRef{
		Collection: collection,
		Id:         id,
	}
}

// MakeID ...
func MakeID(id string) (bson.ObjectId, error) {
	if CheckID(id) == false {
		return "", errors.New("invalid id")
	}
	return bson.ObjectIdHex(id), nil
}

// CheckID ...
func CheckID(id string) bool {
	if len(id) != 24 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] >= 'a' && id[i] <= 'f' {
			continue
		}
		if id[i] >= '0' && id[i] <= '9' {
			continue
		}
		return false
	}
	return true
}

// Regex ...
func Regex(regex string) map[string]interface{} {
	return M{
		"$regex": regex,
	}
}
