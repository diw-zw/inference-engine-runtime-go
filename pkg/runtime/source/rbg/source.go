package rbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	client       dynamic.Interface
	namespace    string
	name         string
	labelFile    string
	fileSnapshot bool
	replayTTL    time.Duration
	suppressMu   sync.Mutex
	suppressed   map[runtime.Signal]time.Time
	pollEvery    time.Duration
	logger       *slog.Logger
}

// Option configures a Source.
type Option func(*Source)

// WithLogger sets the source logger. The default is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(s *Source) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithLabelFile switches the wake-up mechanism from an informer to polling
// a downwardAPI label file. The file must project the Pod's
// metadata.labels (the RoleInstance controller mirrors signalLabels keys
// onto member Pods). By default the file is only a change trigger and the
// snapshot still GETs the RoleInstance; see WithFileSnapshot.
func WithLabelFile(path string) Option {
	return func(s *Source) {
		if path != "" {
			s.labelFile = path
		}
	}
}

// WithPollInterval sets the label-file polling interval (default 1s). It
// only applies when a label file is configured.
func WithPollInterval(interval time.Duration) Option {
	return func(s *Source) { s.pollEvery = interval }
}

// WithFileSnapshot makes the label file the snapshot basis as well as the
// wake-up trigger. This is the low-API-load mode: the executor performs no
// GET until an acknowledgement. Weakened semantics, accepted deliberately:
//
//   - the trigger condition degrades to "label present on the Pod": the
//     file carries neither hook-state nor lifecycle declaration. During
//     controller-side cleanup (hook removal or marker key/value change) a
//     stale file can trigger a late callback; acknowledgement returns
//     Stale and the runtime converges, but callbacks must be idempotent
//     and phase-tolerant, which the Callback contract already requires.
//   - after an acknowledgement the mirrored label lingers in the file
//     until the controller and kubelet propagate the removal; a replay
//     cache suppresses such signals for WithReplaySuppressTTL.
//
// Acknowledgement still GETs the RoleInstance and validates hook-state,
// declaration, and label value before patching, so write-back safety is
// unchanged.
func WithFileSnapshot() Option {
	return func(s *Source) { s.fileSnapshot = true }
}

// WithReplaySuppressTTL sets how long a signal stays hidden from file
// snapshots after it was acknowledged or observed as stale (default 30s).
// It only applies in file-snapshot mode and must cover the RI-to-Pod
// mirror plus kubelet file propagation.
func WithReplaySuppressTTL(ttl time.Duration) Option {
	return func(s *Source) { s.replayTTL = ttl }
}

// New creates a source bound to a single RoleInstance. By default it uses
// an informer that watches only that object via a metadata.name field
// selector. Execution and acknowledgement always use direct GETs; see
// WithLabelFile for the file-based wake-up mode.
func New(client dynamic.Interface, namespace string, name string, opts ...Option) (*Source, error) {
	if client == nil {
		return nil, errors.New("rbg: dynamic client must not be nil")
	}
	if namespace == "" {
		return nil, errors.New("rbg: RoleInstance namespace must not be empty")
	}
	if name == "" {
		return nil, errors.New("rbg: RoleInstance name must not be empty")
	}
	source := &Source{
		client:    client,
		namespace: namespace,
		name:      name,
		replayTTL: 30 * time.Second,
		pollEvery: time.Second,
		logger:    slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(source)
		}
	}
	if source.labelFile != "" && source.pollEvery <= 0 {
		return nil, errors.New("rbg: label-file polling interval must be positive")
	}
	if source.fileSnapshot && source.labelFile == "" {
		return nil, errors.New("rbg: file snapshot requires WithLabelFile")
	}
	if source.fileSnapshot && source.replayTTL <= 0 {
		return nil, errors.New("rbg: replay suppression TTL must be positive")
	}
	return source, nil
}

// Run observes until ctx is done. Notifications are coalesced and never
// carry execution state. Run returns nil once ctx is canceled.
func (s *Source) Run(ctx context.Context, notify chan<- struct{}) error {
	if s.labelFile != "" {
		return s.runLabelFile(ctx, notify)
	}
	return s.runInformer(ctx, notify)
}

// runInformer wakes up on RoleInstance changes. The informer is only a
// wake-up mechanism; the execution basis is always a direct GET.
func (s *Source) runInformer(ctx context.Context, notify chan<- struct{}) error {
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

// runLabelFile wakes up on label-file changes instead of watching the API
// server. The file is expected to be a downwardAPI projection of the Pod's
// metadata.labels. Only its raw content is compared: the runtime never
// treats the file as an execution basis (hook-state and lifecycle
// declarations are not mirrored to Pods), so a change here only triggers a
// fresh Snapshot, which GETs the RoleInstance directly. This removes the
// LIST/WATCH connection to the API server at the cost of wake-up latency
// bounded by the RI-to-Pod mirror and kubelet file propagation.
func (s *Source) runLabelFile(ctx context.Context, notify chan<- struct{}) error {
	last, err := os.ReadFile(s.labelFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			s.logger.Warn("read label file failed", "path", s.labelFile, "error", err)
		}
		last = nil
	}

	ticker := time.NewTicker(s.pollEvery)
	defer ticker.Stop()
	var lastError string
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			content, err := os.ReadFile(s.labelFile)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				// Throttle identical read errors; a missing file is a
				// normal state before the volume is mounted.
				if err.Error() != lastError {
					s.logger.Warn("read label file failed", "path", s.labelFile, "error", err)
					lastError = err.Error()
				}
				continue
			}
			if err == nil {
				lastError = ""
			} else {
				content = nil
			}
			if bytes.Equal(content, last) {
				continue
			}
			last = content
			notifySource(notify)
		}
	}
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
	if s.fileSnapshot {
		return s.snapshotFromLabelFile()
	}
	object, err := s.get(ctx)
	if err != nil {
		return runtime.SourceSnapshot{}, err
	}
	return s.buildSnapshot(object)
}

// snapshotFromLabelFile builds the snapshot from the projected Pod labels.
// In this mode "executable signal" degrades to "label present with the
// declared value" (see WithFileSnapshot). The replay cache hides signals
// that were recently acknowledged or observed as stale while the mirrored
// file still shows them.
func (s *Source) snapshotFromLabelFile() (runtime.SourceSnapshot, error) {
	content, err := os.ReadFile(s.labelFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A missing file is an empty observation: no signal is
			// executable, which is a valid state.
			return runtime.SourceSnapshot{Token: &observation{signals: map[runtime.Signal]bool{}}}, nil
		}
		return runtime.SourceSnapshot{}, fmt.Errorf("rbg: read label file %s: %w", s.labelFile, err)
	}

	labels, malformed := parseDownwardAPILabels(string(content))
	if len(malformed) > 0 {
		s.logger.Warn("skipping malformed label file lines", "path", s.labelFile, "count", len(malformed))
	}

	now := time.Now()
	token := &observation{signals: map[runtime.Signal]bool{}}
	signals := make([]runtime.Signal, 0, len(labels))
	for key, value := range labels {
		signal := runtime.Signal{Key: key, Value: value}
		if s.isSuppressed(signal, now) {
			continue
		}
		token.signals[signal] = true
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

// parseDownwardAPILabels parses the downwardAPI label file format: one
// `key="value"` pair per line. Malformed lines are skipped and returned so
// callers can log them; missing keys and invalid separators never panic.
func parseDownwardAPILabels(content string) (labels map[string]string, malformed []string) {
	labels = map[string]string{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		index := strings.Index(line, "=")
		if index <= 0 {
			malformed = append(malformed, line)
			continue
		}
		key := strings.TrimSpace(line[:index])
		value := strings.TrimSpace(line[index+1:])
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		if key == "" {
			malformed = append(malformed, line)
			continue
		}
		labels[key] = value
	}
	return labels, malformed
}

// suppressSignal hides a signal from file snapshots until the TTL expires,
// covering the mirror propagation window after an acknowledgement or a
// stale observation.
func (s *Source) suppressSignal(signal runtime.Signal) {
	s.suppressMu.Lock()
	defer s.suppressMu.Unlock()
	if s.suppressed == nil {
		s.suppressed = map[runtime.Signal]time.Time{}
	}
	s.suppressed[signal] = time.Now()
}

func (s *Source) isSuppressed(signal runtime.Signal, now time.Time) bool {
	s.suppressMu.Lock()
	defer s.suppressMu.Unlock()
	ackedAt, present := s.suppressed[signal]
	if !present {
		return false
	}
	if now.Sub(ackedAt) > s.replayTTL {
		delete(s.suppressed, signal)
		return false
	}
	return true
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
// observation: the RoleInstance UID, hook-state, current lifecycle
// declaration, and label value must all be unchanged. File-based snapshots
// carry no original UID or hook-state (see WithFileSnapshot), so their
// acknowledgement validates the current state only. Stale observations
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
			s.suppressSignal(signal)
			return runtime.Stale, nil
		}

		patch, err := buildLabelRemovalPatch(object.GetResourceVersion(), signal.Key)
		if err != nil {
			return 0, err
		}
		_, err = s.client.Resource(RoleInstanceGVR).Namespace(s.namespace).Patch(
			ctx, s.name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			s.suppressSignal(signal)
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
	labels := object.GetLabels()
	hookState := labels[HookStateLabel]
	if token.uid != "" {
		// API-based observation: compare against the original.
		if object.GetUID() != token.uid {
			return true, nil
		}
		if hookState != token.hookState {
			return true, nil
		}
		// File-based observations carry no original UID or hook-state:
		// the current-state checks below are the whole validation.
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
