package test

import (
	"fmt"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/replicatedb"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"
)

type ReplicatedbTestSuite struct {
	suite.Suite
	replicatedbConfig *replicatedb.Config
	replicatedbFerry  *replicatedb.ReplicatedbFerry
	ferry             *ghostferry.Ferry
}

const (
	testSchemaName    = "gftest"
	testTableName     = "test_table_1"
)

func (t *ReplicatedbTestSuite) SetupTest() {
	t.replicatedbConfig = &replicatedb.Config{
		Config: testhelpers.NewTestConfig(),
		DatabaseWhitelist: []string{testSchemaName},
	}

	err := t.replicatedbConfig.InitializeAndValidateConfig()
	t.Require().Nil(err)

	t.replicatedbFerry = replicatedb.NewFerry(t.replicatedbConfig)
	err = t.replicatedbFerry.Initialize()
	t.Require().Nil(err)

	t.ferry = t.replicatedbFerry.Ferry

	testhelpers.SeedInitialData(t.ferry.SourceDB, testSchemaName, testTableName, 10)
}

func (t *ReplicatedbTestSuite) TearDownTest() {
	_, err := t.ferry.SourceDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)
	_, err = t.ferry.TargetDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", testSchemaName))
	t.Require().Nil(err)
}

func TestReplicatedb(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &ReplicatedbTestSuite{})
}
