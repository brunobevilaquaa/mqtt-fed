package application

import (
	"fmt"
	"github.com/hashicorp/golang-lru"
	paho "mqtt-fed/infra/queue"
	"reflect"
	"time"
)

type TopicWorkerHandle struct {
	FederatedTopic string
	Channel        chan Message
}

func (t TopicWorkerHandle) Dispatch(msg Message) {
	t.Channel <- msg
}

func NewTopicWorkerHandle(federatedTopic string, ctx *FederatorContext) *TopicWorkerHandle {
	channel := make(chan Message)

	worker := NewTopicWorker(federatedTopic, ctx, channel)
	go worker.Run()

	return &TopicWorkerHandle{
		FederatedTopic: federatedTopic,
		Channel:        channel,
	}
}

type CoreBroker struct {
	Id                   int64
	LatestSeqn           int
	Dist                 int
	LastHeard            time.Time
	Parents              []Parent
	HasUnansweredParents bool
}

type Core struct {
	Myself Announcer
	Other  CoreBroker
}

type Parent struct {
	Id          int64
	WasAnswered bool
}

type TopicWorker struct {
	Topic        string
	Ctx          *FederatorContext
	Channel      chan Message
	Cache        *lru.Cache
	NextId       int
	LatestBeacon time.Time
	CurrentCore  Core
	Children     map[int64]time.Time
}

func (t TopicWorker) Run() {
	for msg := range t.Channel {
		if msg.Type == "RoutedPub" {
			t.handleRoutedPub(msg.RoutedPub)
		} else if msg.Type == "FederatedPub" {
			t.handleFederatedPub(msg.FederatedPub)
		} else if msg.Type == "CoreAnn" {
			t.handleCoreAnn(msg.CoreAnn)
		} else if msg.Type == "MeshMembAnn" {
			t.handleMembAnn(msg.MeshMembAnn)
		} else if msg.Type == "Beacon" {
			t.handleBeacon()
		}
	}
}

func (t *TopicWorker) handleRoutedPub(routedPub RoutedPub) {
	if t.Cache.Contains(routedPub.PubId) {
		return
	}

	t.Cache.Add(routedPub.PubId, true)

	if t.hasLocalSub() {
		fmt.Println("sending pub to local subs")

		_, err := t.Ctx.HostClient.Publish(t.Topic, string(routedPub.Payload), 2, false)

		if err != nil {
			fmt.Println("Error while send to local subscribers ", err)
		}
	}

	senderId := routedPub.SenderId
	routedPub.SenderId = t.Ctx.Id

	topic, replieRoutedPub := routedPub.Serialize(t.Topic)

	// send to mesh parents
	var parents []int64

	for _, parent := range t.CurrentCore.Other.Parents {
		if senderId != parent.Id {
			parents = append(parents, parent.Id)
		}
	}

	SendTo(topic, replieRoutedPub, parents, t.Ctx.Neighbors)

	// send to mesh children
	var children []int64

	for id, child := range t.Children {
		elapsed := time.Now().Sub(child)

		if id != senderId && elapsed < 3*t.Ctx.CoreAnnInterval {
			children = append(children, id)
		}
	}

	SendTo(topic, replieRoutedPub, children, t.Ctx.Neighbors)
}

func (t *TopicWorker) handleFederatedPub(msg FederatedPub) {
	newId := PubId{
		OriginId: t.Ctx.Id,
		Seqn:     t.NextId,
	}

	t.NextId += 1

	pub := RoutedPub{
		PubId:    newId,
		Payload:  msg.Payload,
		SenderId: t.Ctx.Id,
	}

	topic, routedPub := pub.Serialize(t.Topic)

	t.Cache.Add(newId, true)

	// send to mesh parents
	var parents []int64
	for _, parent := range t.CurrentCore.Other.Parents {
		parents = append(parents, parent.Id)
	}

	SendTo(topic, routedPub, parents, t.Ctx.Neighbors)

	// send to mesh children
	var children []int64

	for id, child := range t.Children {
		elapsed := time.Now().Sub(child)

		if elapsed < 3*t.Ctx.CoreAnnInterval {
			children = append(children, id)
		}
	}

	SendTo(topic, routedPub, children, t.Ctx.Neighbors)
}

func (t *TopicWorker) handleCoreAnn(coreAnn CoreAnn) {
	if coreAnn.CoreId == t.Ctx.Id || coreAnn.SenderId == t.Ctx.Id {
		return
	}

	coreAnn.Dist += 1

	core := FilterValid(t.CurrentCore, t.Ctx.CoreAnnInterval)

	if core != nil {
		currentCoreId := t.Ctx.Id

		if c, ok := core.(CoreBroker); ok {
			currentCoreId = c.Id
		}

		if coreAnn.CoreId == currentCoreId {
			core := core.(CoreBroker)
			// received a core ann with a diferent distance to the core: because we
			// are keeping only parents with same distance, the current parents are no
			// longer valid, so we clean the parents list and add the neighbor from the
			// receiving core ann as unique parent for now
			if coreAnn.Seqn > core.LatestSeqn || coreAnn.Dist <= core.Dist {
				t.CurrentCore.Other.LatestSeqn = coreAnn.Seqn
				t.CurrentCore.Other.Dist = coreAnn.Dist
				t.CurrentCore.Other.LastHeard = time.Now()

				wasAnswered := false

				if hasLocalSub(t.LatestBeacon, t.Ctx) {
					answer(coreAnn, t.Topic, t.Ctx)
					wasAnswered = true
				}

				t.CurrentCore.Other.Parents = t.CurrentCore.Other.Parents[:0]
				t.CurrentCore.Other.Parents = append(t.CurrentCore.Other.Parents, Parent{
					Id:          coreAnn.SenderId,
					WasAnswered: wasAnswered,
				})
				t.CurrentCore.Other.HasUnansweredParents = !wasAnswered

				t.forward(coreAnn)

				// neighbor is not already a parent: make it parent if the redundancy
				// permits or if it has a lower id
			} else if coreAnn.Seqn == core.LatestSeqn || coreAnn.Dist == core.Dist {

				var isParent bool

				for _, p := range core.Parents {
					if p.Id == coreAnn.SenderId {
						isParent = true
					}
				}

				if !isParent {
					if len(core.Parents) <= t.Ctx.Redundancy {
						if len(core.Parents) == t.Ctx.Redundancy {
							// pop parent with larger id to open room for new parent
							t.CurrentCore.Other.Parents = t.CurrentCore.Other.Parents[:len(t.CurrentCore.Other.Parents)-1]
						}

						wasAnswered := false

						if hasLocalSub(t.LatestBeacon, t.Ctx) {
							answer(coreAnn, t.Topic, t.Ctx)
							wasAnswered = true
						}

						parent := Parent{
							Id:          coreAnn.SenderId,
							WasAnswered: wasAnswered,
						}

						t.CurrentCore.Other.Parents = append(t.CurrentCore.Other.Parents, parent)
						t.CurrentCore.Other.HasUnansweredParents = !wasAnswered
					}
				}
			}
		} else if coreAnn.CoreId < currentCoreId {
			fmt.Println(currentCoreId, " Core deposed")
			fmt.Println(coreAnn.CoreId, " New core elected")

			t.Children = make(map[int64]time.Time)

			wasAnswered := false

			if t.hasLocalSub() {
				answer(coreAnn, t.Topic, t.Ctx)
				wasAnswered = true
			}

			var parents []Parent
			parents = append(parents, Parent{
				Id:          coreAnn.SenderId,
				WasAnswered: wasAnswered,
			})

			newCore := Core{
				Other: CoreBroker{
					Id:                   coreAnn.CoreId,
					Parents:              parents,
					LatestSeqn:           coreAnn.Seqn,
					LastHeard:            time.Now(),
					Dist:                 coreAnn.Dist,
					HasUnansweredParents: !wasAnswered,
				},
			}

			t.CurrentCore = newCore

			t.forward(coreAnn)
		}
	} else {
		fmt.Println(coreAnn.CoreId, " new core elected")

		t.Children = make(map[int64]time.Time)

		wasAnswered := false

		if t.hasLocalSub() {
			answer(coreAnn, t.Topic, t.Ctx)
			wasAnswered = true
		}

		var parents []Parent
		parents = append(parents, Parent{
			Id:          coreAnn.SenderId,
			WasAnswered: wasAnswered,
		})

		newCore := Core{
			Other: CoreBroker{
				Id:                   coreAnn.CoreId,
				Parents:              parents,
				LatestSeqn:           coreAnn.Seqn,
				LastHeard:            time.Now(),
				Dist:                 coreAnn.Dist,
				HasUnansweredParents: !wasAnswered,
			},
		}

		t.CurrentCore = newCore

		t.forward(coreAnn)
	}

}

func (t *TopicWorker) handleMembAnn(membAnn MeshMembAnn) {
	if membAnn.CoreId == t.Ctx.Id || membAnn.SenderId == t.Ctx.Id {
		return
	}

	if &t.CurrentCore.Other.Id != nil {
		if membAnn.Seqn == t.CurrentCore.Other.LatestSeqn {
			t.Children[membAnn.SenderId] = time.Now()
			answerParents(&t.CurrentCore.Other, t.Ctx, t.Topic)
		}
	} else {
		t.Children[membAnn.SenderId] = time.Now()
	}

}

func (t *TopicWorker) handleBeacon() {
	t.LatestBeacon = time.Now()

	core := FilterValid(t.CurrentCore, t.Ctx.CoreAnnInterval)

	if core != nil {
		if c, ok := core.(CoreBroker); ok {
			answerParents(&c, t.Ctx, t.Topic)
		}

	} else {
		announcer := NewAnnouncer(t.Topic, t.Ctx)

		t.CurrentCore = Core{
			Myself: *announcer,
		}

		t.Children = make(map[int64]time.Time)
	}

}

func (t TopicWorker) hasLocalSub() bool {
	if !t.LatestBeacon.IsZero() {
		elapsed := time.Now().Sub(t.LatestBeacon)

		return elapsed < 3*t.Ctx.BeaconInterval
	} else {
		return false
	}
}

func (t TopicWorker) forward(coreAnn CoreAnn) {
	pub := CoreAnn{
		Dist:     coreAnn.Dist + 1,
		SenderId: t.Ctx.Id,
		Seqn:     coreAnn.Seqn,
		CoreId:   coreAnn.CoreId,
	}

	topic, myCoreAnn := pub.Serialize(t.Topic)

	for id, ngbrClient := range t.Ctx.Neighbors {
		if id != coreAnn.SenderId {
			_, err := ngbrClient.Publish(topic, string(myCoreAnn), 2, false)
			if err != nil {
				fmt.Println("Error while forward message to ", ngbrClient.ClientID)
			}
		}
	}
}

func hasLocalSub(latestBeacon time.Time, ctx *FederatorContext) bool {
	if !latestBeacon.IsZero() {
		elapsed := time.Now().Sub(latestBeacon)

		return elapsed < 3*ctx.BeaconInterval
	} else {
		return false
	}
}

func NewTopicWorker(federatedTopic string, ctx *FederatorContext, channel chan Message) *TopicWorker {

	cache, _ := lru.New(ctx.CacheSize)

	return &TopicWorker{
		Topic:    federatedTopic,
		Ctx:      ctx,
		Channel:  channel,
		Cache:    cache,
		NextId:   0,
		Children: make(map[int64]time.Time),
	}
}

func FilterValid(core Core, coreAnnInterval time.Duration) interface{} {
	if !reflect.DeepEqual(core.Other, CoreBroker{}) {
		elapsed := time.Now().Sub(core.Other.LastHeard)
		if elapsed < 3*coreAnnInterval {
			return core.Other
		}
	} else if !reflect.DeepEqual(core.Myself, Announcer{}) {
		return core.Myself
	}

	return nil
}

func SendTo(topic string, message []byte, ids []int64, neighbors map[int64]*paho.Client) {

	if len(ids) <= 0 {
		return
	}

	firstId := ids[0]

	ids = append(ids[:0], ids[0+1:]...)

	for _, id := range ids {

		if neighbors[id] != nil {
			_, err := neighbors[id].Publish(topic, string(message), 2, false)
			if err != nil {
				fmt.Println("problem creating or queuing the message for broker id ", id)
			}
		} else {
			fmt.Println("broker", id, "is not a neighbor")
		}
	}

	if neighbors[firstId] != nil {
		_, err := neighbors[firstId].Publish(topic, string(message), 2, false)
		if err != nil {
			fmt.Println("problem creating or queuing the message for broker id ", firstId)
		}
	} else {
		fmt.Println("broker", firstId, "is not a neighbor")
	}

}

func answerParents(core *CoreBroker, context *FederatorContext, topic string) {
	if core.HasUnansweredParents {
		pub := MeshMembAnn{
			CoreId:   core.Id,
			Seqn:     core.LatestSeqn,
			SenderId: context.Id,
		}

		topic, myMembAnn := pub.Serialize(topic)

		for _, parent := range core.Parents {
			if !parent.WasAnswered {
				if context.Neighbors[parent.Id] != nil {
					_, err := context.Neighbors[parent.Id].Publish(topic, string(myMembAnn), 2, false)
					if err != nil {
						fmt.Println("error while send my memb ann")
					}
				}

				parent.WasAnswered = true
			}
		}

		core.HasUnansweredParents = false
	}
}

func answer(coreAnn CoreAnn, topic string, context *FederatorContext) {
	pub := MeshMembAnn{
		CoreId:   coreAnn.CoreId,
		Seqn:     coreAnn.Seqn,
		SenderId: context.Id,
	}

	topic, myMembAnn := pub.Serialize(topic)

	if context.Neighbors[coreAnn.SenderId] != nil {
		_, err := context.Neighbors[coreAnn.SenderId].Publish(topic, string(myMembAnn), 2, false)
		if err != nil {
			fmt.Println("error while send my memb ann to ", coreAnn.SenderId)
		}
	} else {
		fmt.Println(coreAnn.SenderId, " is not a neighbor")
	}
}
