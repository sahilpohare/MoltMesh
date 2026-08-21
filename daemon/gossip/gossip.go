package gossip

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/registry"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	maxTopicLength = 256
	maxPayloadSize = 1 << 20
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
	topics map[string]*managedTopic
	mu     sync.Mutex
	log    *zap.Logger
	exec   *appactors.Executor
}

func (m *Manager) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	exec, err := appactors.NewExecutor(ctx, h, "gossip")
	if err != nil {
		return err
	}
	m.exec = exec
	return nil
}

type managedTopic struct {
	topic *pubsub.Topic
	refs  int
}

// New creates a GossipSub manager.
func New(ps *pubsub.PubSub, log *zap.Logger) *Manager {
	return &Manager{
		ps:     ps,
		topics: make(map[string]*managedTopic),
		log:    log,
	}
}

// PublishTaskEvent publishes a TaskEvent to the task's events topic.
func (m *Manager) PublishTaskEvent(ctx context.Context, event *pb.TaskEvent) error {
	if event == nil || event.TaskId == "" {
		return fmt.Errorf("task event requires task_id")
	}
	topic := TaskEventsTopic(event.TaskId)
	data, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// PublishTaskDone publishes task completion to the done topic.
func (m *Manager) PublishTaskDone(ctx context.Context, task *pb.Task) error {
	if task == nil || task.Id == "" {
		return fmt.Errorf("task requires id")
	}
	topic := TaskDoneTopic(task.Id)
	data, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// PublishPresence publishes an agent presence heartbeat.
func (m *Manager) PublishPresence(ctx context.Context, did string, card *pb.AgentCard) error {
	if card == nil || card.Did != did {
		return fmt.Errorf("presence card DID does not match topic")
	}
	if err := registry.VerifyAgentCard(card); err != nil {
		return fmt.Errorf("invalid presence card: %w", err)
	}
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
		if event.TaskId != taskID {
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
		if err := registry.VerifyAgentCard(&card); err != nil {
			m.log.Warn("invalid agent card", zap.Error(err))
			return
		}
		handler(&card)
	})
}

// Publish publishes raw bytes to a named topic. The topic name is used as-is.
func (m *Manager) Publish(ctx context.Context, topic string, data []byte) error {
	if err := validateTopicAndPayload(topic, data); err != nil {
		return err
	}
	return m.publish(ctx, topic, data)
}

// SubscribeTopic subscribes to a named topic and returns a channel of raw payloads.
// The caller must drain or abandon the channel and call the returned cancel func when done.
func (m *Manager) SubscribeTopic(ctx context.Context, topic string) (<-chan []byte, func(), error) {
	if m.exec != nil {
		value, err := m.exec.Call(ctx, func() (any, error) { ch, cancel, err := m.subscribeTopic(ctx, topic); return []any{ch, cancel}, err })
		if err != nil {
			return nil, nil, err
		}
		pair := value.([]any)
		return pair[0].(<-chan []byte), pair[1].(func()), nil
	}
	return m.subscribeTopic(ctx, topic)
}
func (m *Manager) subscribeTopic(ctx context.Context, topic string) (<-chan []byte, func(), error) {
	if err := validateTopicAndPayload(topic, nil); err != nil {
		return nil, nil, err
	}
	t, release, err := m.acquireTopic(topic)
	if err != nil {
		return nil, nil, err
	}
	sub, err := t.Subscribe()
	if err != nil {
		release()
		return nil, nil, fmt.Errorf("subscribe to %q: %w", topic, err)
	}

	ch := make(chan []byte, 64)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer sub.Cancel()
		defer release()
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

func (m *Manager) acquireTopic(name string) (*pubsub.Topic, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.topics[name]; ok {
		entry.refs++
		return entry.topic, m.releaseTopicFunc(name, entry.topic), nil
	}
	t, err := m.ps.Join(name)
	if err != nil {
		return nil, nil, fmt.Errorf("join topic %q: %w", name, err)
	}
	m.topics[name] = &managedTopic{topic: t, refs: 1}
	return t, m.releaseTopicFunc(name, t), nil
}

func (m *Manager) releaseTopicFunc(name string, topic *pubsub.Topic) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			entry := m.topics[name]
			if entry != nil && entry.topic == topic {
				entry.refs--
				if entry.refs == 0 {
					delete(m.topics, name)
					_ = topic.Close()
				}
			}
			m.mu.Unlock()
		})
	}
}

func (m *Manager) publish(ctx context.Context, topicName string, data []byte) error {
	if m.exec != nil {
		_, err := m.exec.Call(ctx, func() (any, error) { return nil, m.publishRaw(ctx, topicName, data) })
		return err
	}
	return m.publishRaw(ctx, topicName, data)
}
func (m *Manager) publishRaw(ctx context.Context, topicName string, data []byte) error {
	if err := validateTopicAndPayload(topicName, data); err != nil {
		return err
	}
	t, release, err := m.acquireTopic(topicName)
	if err != nil {
		return err
	}
	defer release()
	return t.Publish(ctx, data)
}

func validateTopicAndPayload(topic string, data []byte) error {
	if topic == "" || len(topic) > maxTopicLength || strings.ContainsAny(topic, "\x00\r\n") {
		return fmt.Errorf("invalid topic")
	}
	if len(data) > maxPayloadSize {
		return fmt.Errorf("pubsub payload exceeds %d bytes", maxPayloadSize)
	}
	return nil
}

func (m *Manager) subscribe(ctx context.Context, topicName string, handler func([]byte)) error {
	if m.exec != nil {
		_, err := m.exec.Call(ctx, func() (any, error) { return nil, m.subscribeRaw(ctx, topicName, handler) })
		return err
	}
	return m.subscribeRaw(ctx, topicName, handler)
}
func (m *Manager) subscribeRaw(ctx context.Context, topicName string, handler func([]byte)) error {
	t, release, err := m.acquireTopic(topicName)
	if err != nil {
		return err
	}
	sub, err := t.Subscribe()
	if err != nil {
		release()
		return fmt.Errorf("subscribe to %q: %w", topicName, err)
	}

	go func() {
		defer sub.Cancel()
		defer release()
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
