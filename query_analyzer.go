package ghostferry

import (
	_ "github.com/pingcap/tidb/types/parser_driver" // needed for running the parser

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/sirupsen/logrus"
)

type SchemaEvent struct {
	// The SQL statement of the event as returned by the SQL statement parser
	SchemaStatement  string
	// Does this event modify the structure/schema of the DB? ALTER or CREATE table statements alter the schema,
	// but TRUNCATE does not. RENAME statements also alter the structure of which tables are available, and are
	// thus also considered schema changes
	IsSchemaChange   bool
	// "affected" is always the table on which an event operates on (created, deleted, truncated, altered, etc.)
	// For the corner-case of a renamed table, it refers to the original table.
	// The idea is to have one field that exposes what tables existed previously and that are affected by an
	// it is *NOT* the deleted table
	AffectedTable *QualifiedTableName
	// the created table as part of a create/rename operation. This can overlap with "affected" tables
	CreatedTable  *QualifiedTableName
	// the deleted table as part of a delete/rename operation. This can overlap with "affected" tables
	DeletedTable  *QualifiedTableName
}

type QueryAnalyzer struct {
	sqlParser *parser.Parser
	logger    *logrus.Entry
}

func NewQueryAnalyzer() *QueryAnalyzer {
	q := &QueryAnalyzer{
		sqlParser: parser.New(),
		logger: logrus.WithField("tag", "query_analyzer"),
	}
	return q
}

func (q *QueryAnalyzer) ParseSchemaChanges(sqlStatement string, schemaOfStatement string) ([]*SchemaEvent, error) {
	// NOTE: SQL is tricky! You can create a table in a database using a prefix
	// and dot - but you can also create tables with dots in the name.
	//
	// For example, in mysql
	//
	//    CREATE TABLE mydb.mytable ...
	//
	// creates a table called "mydb.mytable" in the current schema, whereas
	//
	//    CREATE TABLE `mydb`.`mytable` ...
	//
	// will create a table called "mytable" in a DB called "mydb". Thus, we need
	// to parse the statement fully to understand what is happening
	stmts, _, err := q.sqlParser.Parse(string(sqlStatement), "", "")
	if err != nil {
		return nil, err
	}

	schemaEvents := make([]*SchemaEvent, 0)
	for _, stmt := range stmts {
		switch t := stmt.(type) {
		case *ast.RenameTableStmt:
			for _, tableInfo := range t.TableToTables {
				schemaOfTable := tableInfo.NewTable.Schema.String()
				if schemaOfTable == "" {
					schemaOfTable = schemaOfStatement
				}
				createdTable := NewQualifiedTableName(schemaOfTable, tableInfo.NewTable.Name.String())
				schemaOfTable = tableInfo.OldTable.Schema.String()
				if schemaOfTable == "" {
					schemaOfTable = schemaOfStatement
				}
				deletedTable := NewQualifiedTableName(schemaOfTable, tableInfo.OldTable.Name.String())
				schemaChange := &SchemaEvent{
					SchemaStatement:  stmt.Text(),
					IsSchemaChange:   true,
					CreatedTable:     &createdTable,
					DeletedTable:     &deletedTable,
					AffectedTable:    &deletedTable,
				}
				schemaEvents = append(schemaEvents, schemaChange)
			}
		case *ast.DropTableStmt:
			for _, table := range t.Tables {
				schemaOfTable := table.Schema.String()
				if schemaOfTable == "" {
					schemaOfTable = schemaOfStatement
				}
				droppedTable := NewQualifiedTableName(schemaOfTable, table.Name.String())
				schemaChange := &SchemaEvent{
					SchemaStatement: stmt.Text(),
					IsSchemaChange:   true,
					AffectedTable:   &droppedTable,
					DeletedTable:    &droppedTable,
				}
				schemaEvents = append(schemaEvents, schemaChange)
			}
		case *ast.CreateTableStmt:
			schemaOfTable := t.Table.Schema.String()
			if schemaOfTable == "" {
				schemaOfTable = schemaOfStatement
			}
			createdTable := NewQualifiedTableName(schemaOfTable, t.Table.Name.String())
			schemaChange := &SchemaEvent{
				SchemaStatement:  stmt.Text(),
				IsSchemaChange:   true,
				CreatedTable:     &createdTable,
				AffectedTable:    &createdTable,
			}
			schemaEvents = append(schemaEvents, schemaChange)
		case *ast.AlterTableStmt:
			schemaOfTable := t.Table.Schema.String()
			if schemaOfTable == "" {
				schemaOfTable = schemaOfStatement
			}
			alteredTable := NewQualifiedTableName(schemaOfTable, t.Table.Name.String())
			schemaChange := &SchemaEvent{
				SchemaStatement: stmt.Text(),
				IsSchemaChange:  true,
				AffectedTable:   &alteredTable,
			}
			schemaEvents = append(schemaEvents, schemaChange)
		case *ast.TruncateTableStmt:
			schemaOfTable := t.Table.Schema.String()
			if schemaOfTable == "" {
				schemaOfTable = schemaOfStatement
			}
			truncatedTable := NewQualifiedTableName(schemaOfTable, t.Table.Name.String())
			schemaChange := &SchemaEvent{
				SchemaStatement: stmt.Text(),
				AffectedTable:   &truncatedTable,
			}
			schemaEvents = append(schemaEvents, schemaChange)
		}
	}

	return schemaEvents, nil
}
