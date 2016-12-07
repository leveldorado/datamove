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
		close(ch)
	}()
	return ch
}

func (m *Mover) MoveDatabase(database string) chan error {
	ch := make(chan error, 1)
	go func() {
		db := m.source.DB(database)
		collections, err := db.CollectionNames()
		if err != nil {
			ch <- err
			close(ch)
			return
		}
		chansList := make([]chan error, len(collections))
		for _, col := range collections {
			chansList = append(chansList, m.MoveCollection(database, col))
		}
		for _, v := range chansList {
			if err := <-v; err != nil {
				ch <- err
			}
		}
		ch <- nil
		close(ch)
	}()
	return ch
}

func (m *Mover) MoveDatabases(databases []string) chan error {
	ch := make(chan error, 1)
	go func() {
		chansList := make([]chan error, len(databases))
		for _, v := range databases {
			chansList = append(chansList, m.MoveDatabase(v))
		}
		for _, c := range chansList {
			for err := range c {
				if err != nil {
					ch <- err
				}
			}
		}
		ch <- nil
		close(ch)
	}()
	return ch
}

func (m *Mover) MoveAllDatabases() chan error {
	databases, err := m.source.DatabaseNames()
	if err != nil {
		ch := make(chan error, 1)
		ch <- err
		close(ch)
		return ch
	}
	return m.MoveDatabases(databases)
}
