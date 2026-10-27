// composite_test.go contains the tests for the composite results.
package arrowio

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// compositeFixture is the composite fixture type.
type compositeFixture struct {
	*testutil.Results
	parts []model.Results
}

// Parts returns the parts for the composite fixture.
func (r *compositeFixture) Parts() []model.Results { return r.parts }

// Cross-package composition must stop on a child error without taking over
// ownership of the child readers or consuming a later tier after a failure.
func TestCompositeResultsErrorAndOwnership(t *testing.T) {
	for _, failingPart := range []int{0, 1} {
		parts := []*testutil.Results{
			{Records: []*model.Record{{Timestamp: time.Unix(0, 1), Name: "cold", Value: 1}}},
			{Records: []*model.Record{{Timestamp: time.Unix(0, 2), Name: "hot", Value: 2}}},
		}
		parts[failingPart].ErrValue = context.DeadlineExceeded
		result := &compositeFixture{Results: &testutil.Results{}, parts: []model.Results{parts[0], parts[1]}}
		var body bytes.Buffer
		if _, err := WriteIPC(&body, result, CompressionZstd); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("part %d: error=%v", failingPart, err)
		}
		if failingPart == 0 && len(parts[1].Records) != 1 {
			t.Fatal("consumed the second part after the first failed")
		}
		if result.Closed || parts[0].Closed || parts[1].Closed {
			t.Fatal("encoder closed results owned by the caller")
		}
	}
}
