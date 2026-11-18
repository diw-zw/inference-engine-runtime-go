package rbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	kcache "k8s.io/client-go/tools/cache"

	"github.com/diw-zw/inference-engine-runtime-go/pkg/runtime"
)

// observation is the opaque snapshot token: the credential of exactly one
// observation of the target RoleInstance. The core never parses it.
type observation struct {
	uid             types.UID
	hookState       string
	resourceVersion string
	signals         map[runtime.Signal]bool
}

// Source observes one RoleInstance in one namespace and acknowledges
// completed signals by removing only the labels this runtime executed.
type Source struct {
	client    dynamic.Interface
	namespace string
	name      string
	logger    *slog.Logger
}

// New creates a source bound to a single RoleInstance. The informer watches
// only that object via a metadata.name field selector; execution and
// acknowledgement always use direct GETs.
func New(client dynamic.Interface, namespace string, name string, logger *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("rbg: dynamic client must not be nil")
	}
	if namespace == "" {
		return nil, errors.New("rbg: RoleInstance namespace must not be empty")
	}
	if name == "" {
		return nil, errors.New("rbg: RoleInstance name must not be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Source{client: client, namespace: namespace, name: name, logger: logger}, nil
}

// Run watches the RoleInstance until ctx is done. The informer is only a
// wake-up mechanism: notifications are coalesced and never carry execution
// state. Run returns nil once ctx is canceled.
func (s *Source) Run(ctx context.Context, notify chan<- struct{}) error {
	tweak := func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", s.name).String()
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.client, 0, s.namespace, tweak)
	informer := factory.ForResource(RoleInstanceGVR).Informer()

	handler := kcache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { notifySource(notify) },
		UpdateFunc: func(_, _ any) { notifySource(notify) },
		DeleteFunc: func(any) { notifySource(notify) },
	}
	if _, err := informer.AddEventHandler(handler); err != nil {
		return fmt.Errorf("rbg: register informer handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !kcache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil
	}
	<-ctx.Done()
	return nil
}

func notifySource(notify chan<- struct{}) {
	select {
	case notify <- struct{}{}:
	default:
	}
}

// Snapshot returns the currently executable signals via a direct GET. It
// fails closed on unknown hook-state values, malformed lifecycle snapshots,
// and keys declared by more than one hook.
func (s *Source) Snapshot(ctx context.Context) (runtime.SourceSnapshot, error) {
	object, err := s.get(ctx)
	if err != nil {
		return runtime.SourceSnapshot{}, err
	}
	return s.buildSnapshot(object)
}

func (s *Source) get(ctx context.Context) (*unstructured.Unstructured, error) {
	object, err := s.client.Resource(RoleInstanceGVR).Namespace(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("rbg: get RoleInstance %s/%s: %w", s.namespace, s.name, err)
	}
	return object, nil
}

// buildSnapshot interprets one observed RoleInstance.
func (s *Source) buildSnapshot(object *unstructured.Unstructured) (runtime.SourceSnapshot, error) {
	labels := object.GetLabels()
	token := &observation{
		uid:             object.GetUID(),
		resourceVersion: object.GetResourceVersion(),
		signals:         map[runtime.Signal]bool{},
	}

	hookState, hasState := labels[HookStateLabel]
	if !hasState || hookState == "" {
		// No lifecycle operation is active: nothing is executable.
		return runtime.SourceSnapshot{Token: token}, nil
	}
	if !knownHookStates[hookState] {
		return runtime.SourceSnapshot{}, fmt.Errorf(
			"rbg: RoleInstance %s/%s has unknown hook-state %q; failing closed",
			s.namespace, s.name, hookState)
	}
	token.hookState = hookState

	hook, executable := executableHooks[hookState]
	if !executable {
		// Updating, Recovering, and Terminating are valid but not
		// executable.
		return runtime.SourceSnapshot{Token: token}, nil
	}

	parsed, err := parseLifecycle(object)
	if err != nil {
		return runtime.SourceSnapshot{}, err
	}
	hookConfig := parsed.hookFor(hook)
	if hookConfig == nil {
		return runtime.SourceSnapshot{Token: token}, nil
	}
	for key, wantValue := range hookConfig.SignalLabels {
		// A missing label never matches; a present label with an empty
		// value matches only an empty declared value.
		labelValue, present := labels[key]
		if present && labelValue == wantValue {
			signal := runtime.Signal{Key: key, Value: wantValue}
			token.signals[signal] = true
		}
	}

	signals := make([]runtime.Signal, 0, len(token.signals))
	for signal := range token.signals {
		signals = append(signals, signal)
	}
	sort.Slice(signals, func(i, j int) bool {
		if signals[i].Key != signals[j].Key {
			return signals[i].Key < signals[j].Key
		}
		return signals[i].Value < signals[j].Value
	})
	return runtime.SourceSnapshot{Signals: signals, Token: token}, nil
}

// parseLifecycle strictly decodes spec.lifecycle and rejects keys declared
// by more than one hook.
func parseLifecycle(object *unstructured.Unstructured) (*lifecycle, error) {
	raw, found, err := unstructured.NestedMap(object.Object, "spec", "lifecycle")
	if err != nil {
		return nil, fmt.Errorf("rbg: invalid spec.lifecycle on RoleInstance %s/%s: %w",
			object.GetNamespace(), object.GetName(), err)
	}
	parsed := &lifecycle{}
	if !found || raw == nil {
		return parsed, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("rbg: encode spec.lifecycle: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(parsed); err != nil {
		return nil, fmt.Errorf("rbg: invalid spec.lifecycle on RoleInstance %s/%s: %w",
			object.GetNamespace(), object.GetName(), err)
	}

	seenKeys := map[string]string{}
	for hookName, hook := range map[string]*lifecycleHook{
		"preUpdate":  parsed.PreUpdate,
		"preRecover": parsed.PreRecover,
		"postReady":  parsed.PostReady,
		"preDelete":  parsed.PreDelete,
	} {
		if hook == nil {
			continue
		}
		for key := range hook.SignalLabels {
			if owner, exists := seenKeys[key]; exists {
				return nil, fmt.Errorf(
					"rbg: signal key %q declared by both %s and %s on RoleInstance %s/%s; failing closed",
					key, owner, hookName, object.GetNamespace(), object.GetName())
			}
			seenKeys[key] = hookName
		}
	}
	return parsed, nil
}

// Ack removes the label for one completed signal after revalidating the
// original observation: the RoleInstance UID, hook-state, current lifecycle
// declaration, and label value must all be unchanged. Stale observations
// never write. The write is a merge patch carrying the latest
// resourceVersion; conflicts are retried after a fresh GET.
func (s *Source) Ack(ctx context.Context, snapshot runtime.SourceSnapshot, signal runtime.Signal) (runtime.AckStatus, error) {
	token, ok := snapshot.Token.(*observation)
	if !ok || token == nil {
		return 0, errors.New("rbg: acknowledgement requires a snapshot from this source")
	}
	if !token.signals[signal] {
		return 0, fmt.Errorf("rbg: signal %s=%s was not part of the observed snapshot", signal.Key, signal.Value)
	}

	for attempt := 0; attempt < maxAckConflicts; attempt++ {
		object, err := s.get(ctx)
		if err != nil {
			return 0, err
		}
		stale, err := s.validateForAck(object, token, signal)
		if err != nil {
			return 0, err
		}
		if stale {
			return runtime.Stale, nil
		}

		patch, err := buildLabelRemovalPatch(object.GetResourceVersion(), signal.Key)
		if err != nil {
			return 0, err
		}
		_, err = s.client.Resource(RoleInstanceGVR).Namespace(s.namespace).Patch(
			ctx, s.name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			return runtime.Acknowledged, nil
		}
		if kerrors.IsConflict(err) {
			s.logger.Warn("merge patch conflicted; revalidating", "roleInstance", s.namespace+"/"+s.name, "signalKey", signal.Key)
			continue
		}
		return 0, fmt.Errorf("rbg: patch RoleInstance %s/%s: %w", s.namespace, s.name, err)
	}
	return 0, fmt.Errorf("rbg: acknowledgement for signal %s hit too many conflicts", signal.Key)
}

// validateForAck returns stale=true when the current object no longer
// matches the original observation. It returns an error for protocol errors
// (unknown state, malformed lifecycle) rather than silently writing.
func (s *Source) validateForAck(object *unstructured.Unstructured, token *observation, signal runtime.Signal) (bool, error) {
	if object.GetUID() != token.uid {
		return true, nil
	}
	labels := object.GetLabels()
	hookState := labels[HookStateLabel]
	if hookState != token.hookState {
		return true, nil
	}
	hook, executable := executableHooks[hookState]
	if !executable {
		return true, nil
	}
	parsed, err := parseLifecycle(object)
	if err != nil {
		return false, err
	}
	hookConfig := parsed.hookFor(hook)
	if hookConfig == nil {
		return true, nil
	}
	if want, declared := hookConfig.SignalLabels[signal.Key]; !declared || want != signal.Value {
		return true, nil
	}
	labelValue, present := labels[signal.Key]
	if !present || labelValue != signal.Value {
		return true, nil
	}
	return false, nil
}

func buildLabelRemovalPatch(resourceVersion, key string) ([]byte, error) {
	patch := map[string]any{
		"metadata": map[string]any{
			"resourceVersion": resourceVersion,
			"labels": map[string]any{
				key: nil,
			},
		},
	}
	encoded, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("rbg: build merge patch: %w", err)
	}
	return encoded, nil
}
