package replicatedb

import "github.com/Shopify/ghostferry"

type StaticDbFilter struct {
	Dbs            []string
}

func contains(s []string, item string) bool {
	for _, v := range s {
		if item == v {
			return true
		}
	}

	return false
}

func NewStaticDbFilter(dbs []string) *StaticDbFilter {
	f := &StaticDbFilter{}

	f.Dbs = dbs

	return f
}

func (s *StaticDbFilter) ApplicableDatabases(dbs []string) ([]string, error) {
	applicableDbs := make([]string, 0, len(dbs))

	for _, name := range dbs {
		if contains(s.Dbs, name) {
			applicableDbs = append(applicableDbs, name)
		}
	}

	return applicableDbs, nil
}

func (s *StaticDbFilter) ApplicableTables(tables []*ghostferry.TableSchema) ([]*ghostferry.TableSchema, error) {
	applicableTables := make([]*ghostferry.TableSchema, 0, len(tables))

	for _, tableSchema := range tables {
		// we do not allow filtering tables - it collides with
		// schema create/delete/renames
		if contains(s.Dbs, tableSchema.Schema) {
			applicableTables = append(applicableTables, tableSchema)
		}
	}

	return applicableTables, nil
}
