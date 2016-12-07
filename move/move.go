package move

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Mover struct {
	source *mgo.Session
	target *mgo.Session
}

func NewMover(source, target *mgo.Session) *Mover {
	return &Mover{source: source, target: target}
}

func (m *Mover) MoveCollection(database, collection string) chan error {
	ch := make(chan error, 1)
	go func() {
		sourceCol := m.source.Copy().DB(database).C(collection)
		defer sourceCol.Database.Session.Close()
		targetCol := m.target.Copy().DB(database).C(collection)
		defer targetCol.Database.Session.Close()

		iter := sourceCol.Find(bson.M{}).Iter()
		doc := bson.M{}
		for iter.Next(&doc) {
			if err := targetCol.Insert(doc); err != nil {
				iter.Close()
				ch <- err
				return
			}
			doc = bson.M{}
		}
		ch <- iter.Close()
	}()
	return ch
}
