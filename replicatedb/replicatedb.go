package replicatedb

import (
	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type ReplicatedbFerry struct {
	Ferry         *ghostferry.Ferry
	config        *Config
}

func NewFerry(config *Config) *ReplicatedbFerry {
	ferry := &ghostferry.Ferry{
		Config: config.Config,
		AllowReplicationFromReplia: true,
	}

	return &ReplicatedbFerry{
		Ferry:         ferry,
		config:        config,
	}
}

func (this *ReplicatedbFerry) Initialize() error {
	return this.Ferry.Initialize()
}

func (this *ReplicatedbFerry) Start() error {
	return this.Ferry.Start()
}

func (this *ReplicatedbFerry) Run() {
	logrus.Info("Running ghostferry replication")
	logrus.Info("press CTRL+C or send an interrupt to end this process")
	this.Ferry.Run()
}
