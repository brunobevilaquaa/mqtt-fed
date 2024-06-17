package application

import (
	"fmt"
	"time"
)

type Announcer struct {
	FederatedTopic string
	stop           chan bool
}

func (a Announcer) Drop() {
	a.stop <- true
	fmt.Println("Stop announcing as core")
}

func NewAnnouncer(federatedTopic string, ctx *FederatorContext) *Announcer {
	ann := CoreAnn{
		CoreId:   ctx.Id,
		Seqn:     0,
		Dist:     0,
		SenderId: ctx.Id,
	}

	stop := make(chan bool)

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				time.Sleep(ctx.CoreAnnInterval)

				for _, neighbor := range ctx.Neighbors {

					topic, coreAnn := ann.Serialize(federatedTopic)

					_, err := neighbor.Publish(topic, string(coreAnn), 2, true)
					if err != nil {
						fmt.Println("error while send beacon")
					}
				}

				ann.Seqn += 1
			}
		}
	}()

	fmt.Println("Start announcing as core")

	return &Announcer{
		FederatedTopic: federatedTopic,
		stop:           stop,
	}
}
