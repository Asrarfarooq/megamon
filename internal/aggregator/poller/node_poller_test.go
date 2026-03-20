package poller

import (
	"context"
	"testing"

	slice "example.com/megamon/copied-slice-api/v1beta1"
	"github.com/stretchr/testify/require"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

type fakeGKEClient struct {
	nodePools []*containerv1beta1.NodePool
	err       error
}

func (f *fakeGKEClient) ListNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error) {
	return f.nodePools, f.err
}

func TestPollResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = jobset.AddToScheme(scheme)
	_ = slice.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	fakeGKE := &fakeGKEClient{
		nodePools: []*containerv1beta1.NodePool{},
	}

	provider := NewNodePoller(fakeClient, fakeGKE)
	nodePoolsUp, err := provider.PollResources(context.Background())

	require.NoError(t, err)
	require.NotNil(t, nodePoolsUp)
}
