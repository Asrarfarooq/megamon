package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	slicev1beta1 "example.com/megamon/copied-slice-api/v1beta1"
	"example.com/megamon/internal/aggregator"
	"example.com/megamon/internal/aggregator/utils"
	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/records"
	corev1 "k8s.io/api/core/v1"
)

type WorkloadReconciler struct {
	client.Client
	Name                   string
	Scheme                 *runtime.Scheme
	EventLog               aggregator.EventLog
	LeaderWorkerSetEnabled bool
	SliceEnabled           bool
	UnknownCountThreshold  float64

	SliceOwnerMapConfigMapRef types.NamespacedName

	// sliceOwnerMap tracks the last known owner for each slice.
	// key: slice name, value: owner info
	sliceOwnerMapMtx    sync.RWMutex
	sliceOwnerMap       map[string]records.OwnerInfo
	sliceOwnerMapLoaded bool
}

func (r *WorkloadReconciler) Init() {
	if r.Name == "" {
		r.Name = "workload-reconciler"
	}
	r.sliceOwnerMap = make(map[string]records.OwnerInfo)
}

const sliceOwnerMapKey = "slice-owner-map.json"

func (r *WorkloadReconciler) loadSliceOwnerMap(ctx context.Context) error {
	r.sliceOwnerMapMtx.Lock()
	defer r.sliceOwnerMapMtx.Unlock()

	if r.sliceOwnerMapLoaded {
		return nil
	}

	var cm corev1.ConfigMap
	if err := r.Get(ctx, r.SliceOwnerMapConfigMapRef, &cm); err != nil {
		if errors.IsNotFound(err) {
			r.sliceOwnerMapLoaded = true
			return nil
		}
		return fmt.Errorf("failed to get slice owner map ConfigMap: %w", err)
	}

	if data, ok := cm.Data[sliceOwnerMapKey]; ok {
		if err := json.Unmarshal([]byte(data), &r.sliceOwnerMap); err != nil {
			return fmt.Errorf("failed to unmarshal slice owner map: %w", err)
		}
	}
	r.sliceOwnerMapLoaded = true
	return nil
}

func (r *WorkloadReconciler) saveSliceOwnerMap(ctx context.Context) error {
	r.sliceOwnerMapMtx.RLock()
	data, err := json.Marshal(r.sliceOwnerMap)
	r.sliceOwnerMapMtx.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal slice owner map: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.SliceOwnerMapConfigMapRef.Name,
			Namespace: r.SliceOwnerMapConfigMapRef.Namespace,
		},
	}

	_, err = ctrl.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data[sliceOwnerMapKey] = string(data)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to save slice owner map ConfigMap: %w", err)
	}

	return nil
}

// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// NB: RBAC for slice resources are added via kustomize, do not add kubebuilder annotations here

func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithName("workload-reconciler")
	log.V(3).Info("reconciling workloads")

	if r.SliceEnabled {
		if err := r.loadSliceOwnerMap(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensuring slice owner map is loaded: %w", err)
		}
	}

	now := time.Now()

	// 1. List all resources
	var jobsetList jobset.JobSetList
	var lwsList lws.LeaderWorkerSetList
	var sliceList slicev1beta1.SliceList
	var nodeList corev1.NodeList

	if err := r.List(ctx, &jobsetList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing jobsets: %w", err)
	}

	if r.LeaderWorkerSetEnabled {
		if err := r.List(ctx, &lwsList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing leaderworkersets: %w", err)
		}
	}

	if r.SliceEnabled {
		if err := r.List(ctx, &sliceList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing slices: %w", err)
		}
	}

	if !r.SliceEnabled {
		if err := r.List(ctx, &nodeList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing nodes: %w", err)
		}
	}

	// 2. Compute Upness and update maps
	jobsetsUp := make(map[string]records.Upness)
	jobsetNodesUp := make(map[string]records.Upness)
	lwsUp := make(map[string]records.Upness)
	slicesUp := make(map[string]records.Upness)

	uidMapKey := func(ns, name string) string {
		return fmt.Sprintf("%s/%s", ns, name)
	}
	jobsetUidMap := map[string]string{}

	for _, js := range jobsetList.Items {
		uid := string(js.UID)
		jobsetUidMap[uidMapKey(js.Namespace, js.Name)] = uid

		attrs := utils.ExtractJobSetAttrs(&js)
		specReplicas, readyReplicas := k8sutils.GetJobSetReplicas(&js)
		state, isTerminal := k8sutils.GetJobSetTerminalState(&js)
		expectedDown := isTerminal && state == jobset.JobSetCompleted

		jobsetsUp[uid] = records.Upness{
			ExpectedCount: specReplicas,
			ReadyCount:    readyReplicas,
			Attrs:         attrs,
			Status:        string(state),
			ExpectedDown:  expectedDown,
		}

		if !r.SliceEnabled {
			jobsetNodesUp[uid] = records.Upness{
				ExpectedCount: k8sutils.GetExpectedNodeCount(&js),
				Attrs:         attrs,
			}
		}
	}

	if !r.SliceEnabled {
		// jobset nodes should only be reconciled when slicing is disabled
		for _, node := range nodeList.Items {
			nodeStatus := k8sutils.IsNodeReady(&node)
			if jsNS, jsName := k8sutils.GetJobSetForNode(&node); jsNS != "" && jsName != "" {
				uid, ok := jobsetUidMap[uidMapKey(jsNS, jsName)]
				if !ok {
					continue
				}
				up, ok := jobsetNodesUp[uid]
				if !ok {
					continue
				}
				if nodeStatus == corev1.ConditionTrue {
					up.ReadyCount++
				} else if nodeStatus == corev1.ConditionUnknown {
					up.UnknownCount++
				}
				jobsetNodesUp[uid] = up
			}
		}
	}

	// TODO, not tested
	if r.LeaderWorkerSetEnabled {
		for _, lwsObj := range lwsList.Items {
			uid := string(lwsObj.UID)
			attrs := utils.ExtractLeaderWorkerSetAttrs(&lwsObj)
			expectedReplicas := int32(1)
			if lwsObj.Spec.Replicas != nil {
				expectedReplicas = *lwsObj.Spec.Replicas
			}
			lwsUp[uid] = records.Upness{
				ExpectedCount: expectedReplicas,
				ReadyCount:    lwsObj.Status.ReadyReplicas,
				Attrs:         attrs,
			}
		}
	}

	observedSlices := make(map[string]bool)
	if r.SliceEnabled {
		// Slices are reconciled by observing the current state of Slices and their owners.
		// We maintain a SliceOwnerMap to track the last known owner for each slice, allowing
		// us to distinguish between expected (owner gone/terminal) and unexpected downtime.
		r.sliceOwnerMapMtx.Lock()
		sliceOwnerMapChanged := false
		for _, s := range sliceList.Items {
			observedSlices[s.Name] = true
			attrs := utils.ExtractSliceAttrs(&s)
			owner := records.OwnerInfo{
				Kind:      attrs.SliceOwnerKind,
				Namespace: attrs.SliceOwnerNamespace,
				Name:      attrs.SliceOwnerName,
			}
			if r.sliceOwnerMap[s.Name] != owner {
				r.sliceOwnerMap[s.Name] = owner
				sliceOwnerMapChanged = true
			}

			ownerExists, ownerTerminal, err := r.getOwnerStatus(ctx, attrs.SliceOwnerKind, attrs.SliceOwnerName, attrs.SliceOwnerNamespace)
			if err != nil {
				log.Error(err, "failed to get owner status", "slice", s.Name)
			}

			isTerminating := !s.DeletionTimestamp.IsZero()
			expectedDown := (!ownerExists || ownerTerminal)

			up := records.Upness{
				Attrs:         attrs,
				ExpectedCount: 1,
				ExpectedDown:  expectedDown,
			}

			if !isTerminating {
				for _, cond := range s.Status.Conditions {
					if cond.Type == slicev1beta1.SliceStateConditionType {
						switch cond.Status {
						case metav1.ConditionTrue:
							up.ReadyCount = 1
						case metav1.ConditionUnknown:
							up.UnknownCount = 1
						}
						break
					}
				}
			}
			slicesUp[s.Name] = up
		}

		// Reconcile observed slice state and SliceOwnerMap.
		// If a slice is in the map but not in the observed list, it has been deleted.
		// We then check if its owner is still active. If it is, the slice is marked
		// as down in the event log to record an interruption.
		for name, owner := range r.sliceOwnerMap {
			if !observedSlices[name] {
				ownerExists, ownerNotTerminal, err := r.getOwnerActiveStatus(ctx, owner.Kind, owner.Name, owner.Namespace)
				if err != nil {
					log.Error(err, "failed to get owner active status", "slice", name)
				}

				if ownerExists && ownerNotTerminal {
					// Slice is missing but owner is active -> mark as down in event log
					slicesUp[name] = records.Upness{
						Attrs: records.Attrs{
							SliceName:           name,
							SliceOwnerKind:      owner.Kind,
							SliceOwnerName:      owner.Name,
							SliceOwnerNamespace: owner.Namespace,
						},
						ExpectedCount: 1,
						ExpectedDown:  false, // It's an UNEXPECTED downtime
					}
				} else {
					// Owner is gone or not terminal -> delete from map
					delete(r.sliceOwnerMap, name)
					sliceOwnerMapChanged = true
				}
			}
		}
		r.sliceOwnerMapMtx.Unlock()

		if sliceOwnerMapChanged {
			if err := r.saveSliceOwnerMap(ctx); err != nil {
				log.Error(err, "failed to save slice owner map")
			}
		}
	}

	// 3. Save to Event Log in parallel
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if _, err := r.EventLog.AppendStateChange(gCtx, now, "jobsets.json", jobsetsUp); err != nil {
			log.Error(err, "failed to append jobset state changes")
		}
		return nil
	})

	if !r.SliceEnabled {
		g.Go(func() error {
			if _, err := r.EventLog.AppendStateChange(gCtx, now, "jobset-nodes.json", jobsetNodesUp); err != nil {
				log.Error(err, "failed to append jobset-nodes state changes")
			}
			return nil
		})
	}

	if r.LeaderWorkerSetEnabled {
		g.Go(func() error {
			if _, err := r.EventLog.AppendStateChange(gCtx, now, "leader-worker-sets.json", lwsUp); err != nil {
				log.Error(err, "failed to append lws state changes")
			}
			return nil
		})
	}

	if r.SliceEnabled {
		g.Go(func() error {
			if _, err := r.EventLog.AppendStateChange(gCtx, now, "slices.json", slicesUp); err != nil {
				log.Error(err, "failed to append slice state changes")
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Error(err, "event log parallel update failed")
	}

	return ctrl.Result{}, nil
}

func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		Named(r.Name).
		Watches(&jobset.JobSet{}, r.mapToStatic())

	if r.LeaderWorkerSetEnabled {
		b = b.Watches(&lws.LeaderWorkerSet{}, r.mapToStatic())
	}

	if r.SliceEnabled {
		b = b.Watches(&slicev1beta1.Slice{}, r.mapToStatic())
	} else {
		b = b.Watches(&corev1.Node{}, r.mapToStatic())
	}

	return b.Complete(r)
}

// mapToStatic generates a static event handler that maps any triggered event to a dummy reconciliation request.
// This ensures a full cluster-wide reconciliation of all workloads when any watched resource changes.
func (r *WorkloadReconciler) mapToStatic() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{
					Name:      "dummy-workload-name",
					Namespace: "default",
				},
			}}
		},
	)
}

// getOwnerStatus checks the cluster for a workload owner (e.g. JobSet, LeaderWorkerSet) by its kind, name, and namespace.
// Returns two booleans indicating if the owner exists and if it is in a terminal state, along with any error encountered.
func (r *WorkloadReconciler) getOwnerStatus(ctx context.Context, ownerKind, ownerName, ownerNamespace string) (exists bool, terminal bool, err error) {
	if ownerName == "" || ownerNamespace == "" {
		return false, false, nil
	}

	switch strings.ToLower(ownerKind) {
	case "jobset":
		var js jobset.JobSet
		if err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNamespace}, &js); err == nil {
			_, ownerTerminal := k8sutils.GetJobSetTerminalState(&js)
			return true, ownerTerminal, nil
		} else if !errors.IsNotFound(err) {
			return false, false, err
		}
	case "leaderworkerset":
		if r.LeaderWorkerSetEnabled {
			var lwsObj lws.LeaderWorkerSet
			if err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNamespace}, &lwsObj); err == nil {
				_, ownerTerminal := k8sutils.GetLeaderWorkerSetTerminalState(&lwsObj)
				return true, ownerTerminal, nil
			} else if !errors.IsNotFound(err) {
				return false, false, err
			}
		}
	}

	return false, false, nil
}

// getOwnerActiveStatus wraps getOwnerStatus to return whether an owner currently exists and is actively running.
// Returns two booleans indicating if the owner exists and if it is active (non-terminal), along with any error.
func (r *WorkloadReconciler) getOwnerActiveStatus(ctx context.Context, ownerKind, ownerName, ownerNamespace string) (exists bool, nonTerminal bool, err error) {
	var terminal bool
	exists, terminal, err = r.getOwnerStatus(ctx, ownerKind, ownerName, ownerNamespace)
	nonTerminal = !terminal
	return
}
