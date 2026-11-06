// Package stage writes request-private aggregate input, not warehouse files.
package stage

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

const batchRows = 65536
const batchBytes int64 = 8 << 20

// Writer bounds a batch by rows and uncompressed input bytes. A single large
// record can exceed the batch target, but must fit the request byte budget.
// The caller owns the underlying file and must call Close even after an error.
type Writer struct {
	out                                 *file.Writer
	limit, used, buffered               int64
	times                               []int64
	names, keys, values, kinds, scalars []parquet.ByteArray
	defs, reps                          []int16
	closed                              bool
	failure                             error
}

// New writes Zstd Parquet to out. limit is the request's uncompressed input budget.
// The caller closes out; this writer does not.
func New(out io.Writer, limit int64) (*Writer, error) {
	if limit < 1 {
		return nil, fmt.Errorf("aggregation staging byte budget must be positive")
	}
	text := func(name string) schema.Node {
		n, err := schema.NewPrimitiveNodeLogical(name, parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)
		if err != nil {
			panic(err)
		} // fixed internal schema
		return n
	}
	pair, err := schema.NewGroupNode("key_value", parquet.Repetitions.Repeated, schema.FieldList{text("key"), text("value")}, -1)
	if err != nil {
		return nil, err
	}
	meta, err := schema.NewGroupNodeLogical("meta", parquet.Repetitions.Required, schema.FieldList{pair}, schema.MapLogicalType{}, -1)
	if err != nil {
		return nil, err
	}
	root, err := schema.NewGroupNode("hot", parquet.Repetitions.Required, schema.FieldList{schema.NewInt64Node("time", parquet.Repetitions.Required, -1), text("name"), meta, text("kind"), text("value")}, -1)
	if err != nil {
		return nil, err
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3))
	// Hide an optional io.Closer: the request owns the file, including close errors.
	return &Writer{out: file.NewParquetWriter(struct{ io.Writer }{out}, root, file.WithWriterProps(props)), limit: limit}, nil
}

func (w *Writer) InputBytes() int64 { return w.used }

func (w *Writer) Append(r Row) error {
	if w.closed {
		return fmt.Errorf("hot stage is closed")
	}
	if w.failure != nil {
		return w.failure
	}
	size := InputBytes(r)
	if size > w.limit-w.used {
		return fmt.Errorf("aggregation hot staging exceeds byte budget")
	}
	// Flux emits UTF-8 strings. Reject malformed strings instead of writing an
	// invalid Parquet UTF8 column or silently merging normalized metadata keys.
	if !utf8.ValidString(r.Name) || !utf8.ValidString(r.Kind) || !utf8.ValidString(r.Value) {
		return fmt.Errorf("invalid UTF-8 in hot aggregation input")
	}
	for k, v := range r.Meta {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return fmt.Errorf("invalid UTF-8 in hot aggregation metadata")
		}
	}
	if len(w.times) > 0 && (len(w.times) >= batchRows || size > batchBytes-w.buffered) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.times = append(w.times, r.Time)
	w.names = append(w.names, parquet.ByteArray(r.Name))
	w.kinds = append(w.kinds, parquet.ByteArray(r.Kind))
	w.scalars = append(w.scalars, parquet.ByteArray(r.Value))
	keys := make([]string, 0, len(r.Meta))
	for k := range r.Meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		w.defs = append(w.defs, 0)
		w.reps = append(w.reps, 0)
	}
	for i, k := range keys {
		w.keys = append(w.keys, parquet.ByteArray(k))
		w.values = append(w.values, parquet.ByteArray(r.Meta[k]))
		w.defs = append(w.defs, 1)
		rep := int16(1)
		if i == 0 {
			rep = 0
		}
		w.reps = append(w.reps, rep)
	}
	w.used += size
	w.buffered += size
	return nil
}

func (w *Writer) flush() (err error) {
	if w.failure != nil {
		return w.failure
	}
	if len(w.times) == 0 {
		return nil
	}
	group := w.out.AppendRowGroup()
	defer func() { err = errors.Join(err, group.Close()); w.failure = err }()
	// Columns follow the schema: time, name, map key, map value, kind, scalar.
	for i := 0; i < 6; i++ {
		col, e := group.NextColumn()
		if e != nil {
			return e
		}
		if i == 0 {
			_, err = col.(*file.Int64ColumnChunkWriter).WriteBatch(w.times, nil, nil)
		} else {
			data := w.names
			var defs, reps []int16
			switch i {
			case 2:
				data = w.keys
				defs = w.defs
				reps = w.reps
			case 3:
				data = w.values
				defs = w.defs
				reps = w.reps
			case 4:
				data = w.kinds
			case 5:
				data = w.scalars
			}
			_, err = col.(*file.ByteArrayColumnChunkWriter).WriteBatch(data, defs, reps)
		}
		err = errors.Join(err, col.Close())
		if err != nil {
			return err
		}
	}
	// Drop backing arrays so a previous large batch cannot retain source strings.
	w.times = nil
	w.names = nil
	w.keys = nil
	w.values = nil
	w.kinds = nil
	w.scalars = nil
	w.defs = nil
	w.reps = nil
	w.buffered = 0
	return nil
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	// Flush explicitly so footer errors are observed even if the library Close
	// only reports its sink-close error. Close still releases the writer.
	return errors.Join(w.flush(), w.out.FlushWithFooter(), w.out.Close())
}
