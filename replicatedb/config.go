package replicatedb

import (
	"fmt"
	"github.com/Shopify/ghostferry"
)

type Config struct {
	*ghostferry.Config

	// Whitelisted databases that are considered fro replication
	DatabaseWhitelist []string
}

func (c *Config) InitializeAndValidateConfig() error {
	if len(c.DatabaseWhitelist) == 0 {
		return fmt.Errorf("Need a whitelist of databases to be specified")
	}

	c.TableFilter = NewStaticDbFilter(
		c.DatabaseWhitelist,
	)

	// due to limitations in our re-writing of schema alterations, we currently
	// cannot support re-mapping of databases or tables! Just in case someone
	// thinks the configs between different ghostferry tools (e.g., copydb
	// versus replicatedb), we alert the user the that the config is not fully
	//supported instead of just ignoring it
	if len(c.DatabaseRewrites) + len(c.TableRewrites) > 0 {
		return fmt.Errorf("Replicatedb does not allow database or table rewrites")
	}

	if err := c.Config.ValidateConfig(); err != nil {
		return err
	}

	return nil
}
