/*
 * This file is part of Atlas-DB.
 *
 * Atlas-DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Atlas-DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Atlas-DB. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package consensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bottledcode/atlas-db/atlas/kv"
	"github.com/bottledcode/atlas-db/atlas/options"
	"github.com/bottledcode/atlas-db/atlas/trie"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationSender interface {
	HandleNotifications()
	Notify(migration *Migration) error
	GenerateNotification(migration *Migration) *Migration
}

func DefaultNotificationSender() NotificationSender {
	return sender
}

type notificationSender struct {
	notifications map[string][]*notification
	waiters       map[string]chan struct{}
	mu            sync.Mutex
	subscriptions trie.Trie[*Subscribe]
	notification  chan *notification
}

type notification struct {
	sub *Subscribe
	pub *Notify
}

var notificationHandler = sync.Once{}
var sender = &notificationSender{
	notifications: make(map[string][]*notification),
	waiters:       make(map[string]chan struct{}),
	subscriptions: trie.New[*Subscribe](),
	notification:  make(chan *notification, 10000),
}

func (s *notificationSender) HandleNotifications() {
	notificationHandler.Do(func() {
		go func() {
			for {
				next := <-s.notification
				sender.mu.Lock()
				if list, ok := sender.notifications[next.sub.GetUrl()]; ok {
					sender.notifications[next.sub.GetUrl()] = append(list, next)
					sender.waiters[next.sub.GetUrl()] <- struct{}{}
					sender.mu.Unlock()
					continue
				}

				sender.notifications[next.sub.GetUrl()] = []*notification{next}
				sender.waiters[next.sub.GetUrl()] = make(chan struct{})

				// wait for 100 notifications going to this url or 100ms, whichever is sooner
				go func() {
					timer := time.After(100 * time.Millisecond)
					counter := atomic.Int32{}
					waiter := sender.waiters[next.sub.GetUrl()]
					sender.mu.Unlock()

					for {
						select {
						case <-timer:
							goto wait
						case <-waiter:
							counter.Add(1)
							if counter.Load() >= 100 {
								goto wait
							}
						}
					}
				wait:

					sender.mu.Lock()

					list := sender.notifications[next.sub.GetUrl()]
					delete(sender.notifications, next.sub.GetUrl())
					delete(sender.waiters, next.sub.GetUrl())

					sender.mu.Unlock()

					var nl []*Notify
					for _, n := range list {
						nl = append(nl, n.pub)
					}

					bodyBytes, err := json.Marshal(nl)
					if err != nil {
						options.Logger.Error("failed to marshal notification list", zap.Error(err))
						return
					}

					client := &http.Client{
						Timeout: 2 * time.Second,
					}

					for retries := next.sub.GetOptions().GetRetryAttempts(); retries > 0; retries-- {
						body := bytes.NewReader(bodyBytes)

						req, err := http.NewRequest("POST", next.sub.GetUrl(), body)
						if err != nil {
							options.Logger.Error("failed to create notification request", zap.Error(err))
							return
						}

						resp, err := client.Do(req)
						if err != nil {
							options.Logger.Error("failed to send notification", zap.Error(err))
							return
						}
						_ = resp.Body.Close()
						if resp.StatusCode == http.StatusOK {
							return
						}
						options.Logger.Warn("failed to send notification", zap.Int("status_code", resp.StatusCode))
						retryBase := next.sub.GetOptions().RetryAfterBase.AsDuration()
						if retryBase == 0 {
							retryBase = 100 * time.Millisecond
						}
						time.Sleep(retryBase * time.Duration(next.sub.GetOptions().GetRetryAttempts()-retries+1))
					}
				}()
			}
		}()
	})
}

func (s *notificationSender) Notify(migration *Migration) error {
	key := migration.GetVersion().GetTableName()
	if len(key) == 0 {
		return nil
	}
	prefix := s.currentBucket([]byte(key))
	ctx := context.Background()
	qm := GetDefaultQuorumManager(ctx)
	magicKey := kv.NewKeyBuilder().Meta().Table("magic").Append("pb").Append(string(prefix)).Build()
	q, err := qm.GetQuorum(ctx, string(magicKey))
	if err != nil {
		options.Logger.Error("failed to get quorum for notification", zap.Error(err))
		return errors.New("failed to get quorum for notification")
	}
	resp, err := q.WriteKey(ctx, &WriteKeyRequest{
		Sender: nil,
		Table:  string(magicKey),
		Value:  migration.GetData().GetChange(),
	})
	if err != nil {
		options.Logger.Error("failed to write magic key to quorum", zap.Error(err))
		return errors.New("failed to write magic key to quorum")
	}
	if resp.Error != "" {
		options.Logger.Error("failed to write magic key from quorum", zap.Error(errors.New(resp.Error)))
		return errors.New("failed to write magic key from quorum")
	}
	return nil
}

func (s *notificationSender) GenerateNotification(migration *Migration) *Migration {
	if mig, ok := migration.GetMigration().(*Migration_Data); ok {
		version := fmt.Sprintf("%d:%d:%d", migration.GetVersion().GetMigrationVersion(), migration.GetVersion().GetTableVersion(), migration.GetVersion().GetNodeId())

		switch op := mig.Data.GetChange().GetOperation().(type) {
		case *KVChange_Set:
			change := proto.Clone(migration).(*Migration)
			change.GetData().GetChange().Operation = &KVChange_Notification{
				Notification: &Notify{
					Key: []byte(migration.GetVersion().GetTableName()),
					Change: &Notify_Set{
						Set: op.Set,
					},
					Version: version,
					Ts:      timestamppb.Now(),
				},
			}
			return change
		case *KVChange_Del:
			change := proto.Clone(migration).(*Migration)
			change.GetData().GetChange().Operation = &KVChange_Notification{
				Notification: &Notify{
					Key: []byte(migration.GetVersion().GetTableName()),
					Change: &Notify_Del{
						Del: op.Del,
					},
					Version: version,
					Ts:      timestamppb.Now(),
				},
			}
			return change
		case *KVChange_Acl:
			change := proto.Clone(migration).(*Migration)
			change.GetData().GetChange().Operation = &KVChange_Notification{
				&Notify{
					Key: []byte(migration.GetVersion().GetTableName()),
					Change: &Notify_Acl{
						Acl: op.Acl,
					},
					Version: version,
					Ts:      timestamppb.Now(),
				},
			}
			return change
		}
	}
	return migration
}

func (s *notificationSender) currentBucket(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}

	return key[:1<<(bits.Len(uint(len(key)))-1)]
}

func (s *notificationSender) nextBucket(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}

	shift := bits.Len(uint(len(key))) - 2
	if shift < 0 {
		return nil
	}

	return key[:1<<shift]
}

// maybeHandleMagicKey is meant to be called when applying/replaying migrations
func (s *notificationSender) maybeHandleMagicKey(ctx context.Context, migration *Migration) (bool, error) {
	prefix := kv.NewKeyBuilder().Meta().Table("magic")
	if !strings.HasPrefix(migration.GetVersion().GetTableName(), string(prefix.Build())) {
		return false, nil
	}

	//For now, the only magic key is about notifications, so this is a notification.
	//We have received a notification into a prefix bucket.
	//This means different things for different messages.
	//For a migration containing a:
	// subscription: we append it to the key value and store in our server subscriptions
	// notification: we find matching prefixes and forward the notification, as well as any smaller notification prefixes
	// read/write/acl: we generate a notification, and commit to our key, which triggers the above workflow
	//and that is it.
	//We guarantee at-least-once by not committing to the log until after we have handled notifications. This ensures
	//that notifications do not get lost.
	originalKey := migration.GetVersion().GetTableName()[:len(prefix.Append("pb").Build())]
	key := []byte(migration.GetVersion().GetTableName())

	switch mig := migration.GetMigration().(type) {
	case *Migration_None:
		return false, nil
	case *Migration_Schema:
		return false, nil
	case *Migration_Data:
		switch op := mig.Data.GetChange().GetOperation().(type) {
		case *KVChange_Sub:
			store := kv.GetPool().MetaStore()
			txn, err := store.Begin(true)
			if err != nil {
				return true, err
			}
			defer txn.Discard()
			obj, err := txn.Get(ctx, key)
			var list SubscriptionList
			if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
				return true, err
			}
			if err == nil {
				// Key exists - unmarshal existing list
				err = proto.Unmarshal(obj, &list)
				if err != nil {
					return true, err
				}
			}
			// If key doesn't exist, list remains zero-value (empty list)
			list.Subscriptions = append(list.Subscriptions, op.Sub)
			obj, err = proto.Marshal(&list)
			if err != nil {
				return true, err
			}
			s.subscriptions.Insert(op.Sub.Prefix, op.Sub)
			err = txn.Put(ctx, key, obj)
			if err != nil {
				return true, err
			}
			err = txn.Commit()
			if err != nil {
				return true, err
			}
		case *KVChange_Notification:
			key = append(key, []byte(":log")...)
			store := kv.GetPool().MetaStore()
			txn, err := store.Begin(true)
			if err != nil {
				return true, err
			}
			defer txn.Discard()
			obj, err := txn.Get(ctx, key)
			if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
				return true, err
			}
			if obj != nil {
				var list SubscriptionList
				err = proto.Unmarshal(obj, &list)
				if err != nil {
					return true, err
				}
				log := list.GetLog()
				if slices.IndexFunc(log, func(notify *Notify) bool {
					return bytes.Equal(notify.Key, op.Notification.Key) && notify.Version == op.Notification.Version
				}) > -1 {
					// we have already processed this notification
					return true, nil
				}
				list.Log = append(list.Log, op.Notification)
				obj, err = proto.Marshal(&list)
				if err != nil {
					return true, err
				}

				for _, sub := range s.subscriptions.PrefixesOf(op.Notification.Key) {
					note := &notification{
						sub: sub,
						pub: op.Notification,
					}
					s.notification <- note
				}
				s.HandleNotifications()
			}

			// get the next lower power of two prefix
			nextBucket := s.nextBucket([]byte(originalKey))
			// If nextBucket is nil, we've reached the end of the cascade (single byte key)
			if len(nextBucket) > 0 {
				nextKey := string(prefix.Append("pb").Append(string(nextBucket)).Build())
				qm := GetDefaultQuorumManager(ctx)
				q, err := qm.GetQuorum(ctx, nextKey)
				if err != nil {
					return true, err
				}
				resp, err := q.WriteKey(ctx, &WriteKeyRequest{
					Sender: nil,
					Table:  nextKey,
					Value: &KVChange{
						Operation: &KVChange_Notification{
							Notification: op.Notification,
						},
					},
				})
				if err != nil {
					return true, err
				}
				if resp.Error != "" {
					return true, errors.New(resp.Error)
				}
			}

			if obj != nil {
				err = txn.Put(ctx, key, obj)
				if err != nil {
					return true, err
				}
				err = txn.Commit()
				if err != nil {
					return true, err
				}
			}
		default:
			panic("unsupported migration type")
		}
	default:
		panic("unsupported migration type")
	}
	return false, nil
}
