package gossip

import (
	"context"
	"fmt"
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// Topic name helpers.
func TaskEventsTopic(taskID string) string {
	return fmt.Sprintf("a2a/tasks/%s/events", taskID)
}
func TaskDoneTopic(taskID string) string {
	return fmt.Sprintf("a2a/tasks/%s/done", taskID)
}
func PresenceTopic(did string) string {
	return fmt.Sprintf("a2a/agents/%s/presence", did)
}
func CapabilityTopic(namespace string) string {
	return fmt.Sprintf("a2a/capabilities/%s", namespace)
}

// Manager manages GossipSub topic subscriptions and publishing.
type Manager struct {
	ps     *pubsub.PubSub
	topics map[string]*pubsub.Topic
	mu     sync.Mutex
	log    *zap.Logger
}

// New creates a GossipSub manager.
func New(ps *pubsub.PubSub, log *zap.Logger) *Manager {
	return &Manager{
		ps:     ps,
		topics: make(map[string]*pubsub.Topic),
		log:    log,
	}
}

// PublishTaskEvent publishes a TaskEvent to the task's events topic.
func (m *Manager) PublishTaskEvent(ctx context.Context, event *pb.TaskEvent) error {
	topic := TaskEventsTopic(event.TaskId)
	data, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// PublishTaskDone publishes task completion to the done topic.
func (m *Manager) PublishTaskDone(ctx context.Context, task *pb.Task) error {
	topic := TaskDoneTopic(task.Id)
	data, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// PublishPresence publishes an agent presence heartbeat.
func (m *Manager) PublishPresence(ctx context.Context, did string, card *pb.AgentCard) error {
	topic := PresenceTopic(did)
	data, err := proto.Marshal(card)
	if err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// SubscribeTaskEvents subscribes to task event stream, calling handler for each event.
func (m *Manager) SubscribeTaskEvents(ctx context.Context, taskID string, handler func(*pb.TaskEvent)) error {
	return m.subscribe(ctx, TaskEventsTopic(taskID), func(data []byte) {
		var event pb.TaskEvent
		if err := proto.Unmarshal(data, &event); err != nil {
			m.log.Warn("unmarshal task event", zap.Error(err))
			return
		}
		handler(&event)
	})
}

// SubscribeCapabilities subscribes to capability advertisements for a namespace.
func (m *Manager) SubscribeCapabilities(ctx context.Context, namespace string, handler func(*pb.AgentCard)) error {
	return m.subscribe(ctx, CapabilityTopic(namespace), func(data []byte) {
		var card pb.AgentCard
		if err := proto.Unmarshal(data, &card); err != nil {
			m.log.Warn("unmarshal agent card", zap.Error(err))
			return
		}
		handler(&card)
	})
}

// Publish publishes raw bytes to a named topic. The topic name is used as-is.
func (m *Manager) Publish(ctx context.Context, topic string, data []byte) error {
	return m.publish(ctx, topic, data)
}

// SubscribeTopic subscribes to a named topic and returns a channel of raw payloads.
// The caller must drain or abandon the channel and call the returned cancel func when done.
func (m *Manager) SubscribeTopic(ctx context.Context, topic string) (<-chan []byte, func(), error) {
	t, err := m.getTopic(topic)
	if err != nil {
		return nil, nil, err
	}
	sub, err := t.Subscribe()
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe to %q: %w", topic, err)
	}

	ch := make(chan []byte, 64)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer sub.Cancel()
		defer close(ch)
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			select {
			case ch <- msg.Data:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, cancel, nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (m *Manager) getTopic(name string) (*pubsub.Topic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.topics[name]; ok {
		return t, nil
	}
	t, err := m.ps.Join(name)
	if err != nil {
		return nil, fmt.Errorf("join topic %q: %w", name, err)
	}
	m.topics[name] = t
	return t, nil
}

func (m *Manager) publish(ctx context.Context, topicName string, data []byte) error {
	t, err := m.getTopic(topicName)
	if err != nil {
		return err
	}
	return t.Publish(ctx, data)
}

func (m *Manager) subscribe(ctx context.Context, topicName string, handler func([]byte)) error {
	t, err := m.getTopic(topicName)
	if err != nil {
		return err
	}
	sub, err := t.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribe to %q: %w", topicName, err)
	}

	go func() {
		defer sub.Cancel()
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				m.log.Warn("gossipsub receive", zap.String("topic", topicName), zap.Error(err))
				continue
			}
			handler(msg.Data)
		}
	}()

	return nil
}
