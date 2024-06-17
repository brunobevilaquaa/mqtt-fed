package application

import "time"

type JoinRequest struct {
	Ip string `json:"ip"`
}

type NeighborConfig struct {
	Id int64  `json:"id" yaml:"id"`
	Ip string `json:"ip" yaml:"ip"`
}

type FederatorConfig struct {
	Id              int64            `json:"id" yaml:"id"`
	Host            string           `json:"ip" yaml:"host"`
	Neighbors       []NeighborConfig `json:"neighbors" yaml:"neighbors"`
	CoreAnnInterval time.Duration    `json:"coreAnnInterval" yaml:"core_ann_interval"`
	BeaconInterval  time.Duration    `json:"beaconInterval" yaml:"beacon_interval"`
	Redundancy      int              `json:"redundancy" yaml:"redundancy"`
}

type HTTPResponse struct {
	Status      string      `json:"status"`
	Code        int         `json:"code"`
	Data        interface{} `json:"data"`
	Description string      `json:"description"`
}
