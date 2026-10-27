// response.go contains the code for the arrow response.
package arrowio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	model "beehive-data-api/internal/query"
)

// MIME is the Arrow IPC streaming format.
// It is not the random-access file format, whose footer arrives last.
// random-access file format, whose footer arrives last.
const MIME = "application/vnd.apache.arrow.stream"

// Compression is the compression for the arrow response.
// Belongs to the IPC record-batch buffers, not HTTP Content-Encoding.
// Zstd is the Arrow default. An operator can select none or lz4 at startup.
type Compression string

// Compression constants.
const (
	CompressionNone   Compression = "none"
	CompressionZstd   Compression = "zstd"
	CompressionLZ4    Compression = "lz4"
	CompressionHeader             = "X-Arrow-Compression"
)

// ParseCompression parses the arrow compression.
func ParseCompression(raw string) (Compression, error) {
	switch mode := Compression(strings.ToLower(strings.TrimSpace(raw))); mode {
	case "", CompressionZstd:
		return CompressionZstd, nil
	case CompressionNone:
		return CompressionNone, nil
	case CompressionLZ4:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid Arrow compression %q: expected none, zstd, or lz4", raw)
	}
}

// BatchRows is how many Go records are packed into one IPC batch.
// DuckDB chooses its own batch size on the cold path.
const BatchRows = 65536

// field constants.
const (
	FieldTimestamp = iota
	FieldName
	FieldValue
	FieldMeta
)

// TimestampType is the timestamp for the arrow response.
// Nanoseconds without a zone. DuckDB's TIMESTAMP_NS export
// has no zone either, and a tier-spanning stream needs one type. Instants are
// UTC. The data client localizes this column to UTC.
var TimestampType = &arrow.TimestampType{Unit: arrow.Nanosecond}

// Schema is the schema for the arrow response.
// The body of an Arrow query response. value is one string
// column holding the JSON scalar for that row (11.7, 1024, "file.jpg", true).
// meta is the JSON object for that row's tags, not an Arrow map. Sorting a
// map for a cold month held the result before the first batch. A JSON string
// is the projection that already streamed, and json.loads restores both.
var Schema = arrow.NewSchema([]arrow.Field{
	{Name: "timestamp", Type: TimestampType, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "meta", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// arrowResultContextKey is the context key for the arrow result.
type arrowResultContextKey struct{}

// WithResult adds the arrow result context to the context.
func WithResult(ctx context.Context) context.Context {
	return context.WithValue(ctx, arrowResultContextKey{}, true)
}

// WantsResult checks if the context wants the arrow result.
func WantsResult(ctx context.Context) bool {
	enabled, _ := ctx.Value(arrowResultContextKey{}).(bool)
	return enabled
}

// AcceptsStream checks if the context wants the arrow stream.
// HTTP always serves the same four-column Arrow schema, independent of tier
// and client. Prototype schema parameters cannot expose storage value columns.
func AcceptsStream(accept string) bool {
	for _, part := range strings.Split(accept, ",") {
		media, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil || !strings.EqualFold(media, MIME) {
			continue
		}
		quality := 1.0
		if raw, ok := params["q"]; ok {
			quality, err = strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(quality) || quality <= 0 || quality > 1 {
				continue
			}
		}
		return true
	}
	return false
}

// arrowBatchReader is the arrow batch reader type.
// A Results that can yield API-schema record batches.
// The cold backend implements it. Hot and the dummy backend do not, and the
// handler builds batches from their records.
type arrowBatchReader interface {
	ArrowReader() array.RecordReader
}

// WriteIPC writes the results as an Arrow IPC stream in Schema.
// The caller has already sent the response status.
func WriteIPC(w io.Writer, results model.Results, compression Compression) (n int, err error) {
	opts := []ipc.Option{ipc.WithSchema(Schema)}
	switch compression {
	case CompressionNone:
	case "", CompressionZstd:
		opts = append(opts, ipc.WithZstd(), ipc.WithCompressConcurrency(1))
	case CompressionLZ4:
		opts = append(opts, ipc.WithLZ4(), ipc.WithCompressConcurrency(1))
	default:
		return 0, fmt.Errorf("invalid Arrow compression %q", compression)
	}
	writer := ipc.NewWriter(w, opts...)
	defer func() { err = errors.Join(err, writer.Close()) }()
	return writeArrowAny(writer, results)
}

// writeArrowAny writes the results as an Arrow IPC stream in Schema.
func writeArrowAny(writer *ipc.Writer, results model.Results) (int, error) {
	if composite, ok := results.(model.CompositeResults); ok {
		n := 0
		for _, part := range composite.Parts() {
			count, err := writeArrowAny(writer, part)
			n += count
			if err != nil {
				return n, err
			}
			if err := part.Err(); err != nil {
				return n, err
			}
		}
		return n, nil
	}
	if batches, ok := results.(arrowBatchReader); ok {
		return WriteReader(writer, batches.ArrowReader())
	}
	return writeRecordBatches(writer, results)
}

// WriteReader writes the reader as an Arrow IPC stream in Schema.
func WriteReader(writer *ipc.Writer, rdr array.RecordReader) (int, error) {
	n := 0
	for rdr.Next() {
		rec := rdr.RecordBatch()
		out, err := AdoptBatch(rec)
		if err != nil {
			return n, err
		}
		err = writer.Write(out)
		out.Release()
		if err != nil {
			return n, err
		}
		n += int(rec.NumRows())
	}
	if err := rdr.Err(); err != nil {
		return n, err
	}
	return n, nil
}

// SchemaCompatible reports whether a DuckDB batch schema can be written as Schema.
// A DuckDB batch schema can be written as Schema if it has the same number of columns and fields,
// and the fields have the same names and types.
func SchemaCompatible(schema *arrow.Schema) error {
	if schema == nil {
		return fmt.Errorf("cold arrow schema is missing")
	}
	if schema.NumFields() != len(Schema.Fields()) {
		return fmt.Errorf("cold arrow schema has %d columns, want %d (%s)", schema.NumFields(), len(Schema.Fields()), schema)
	}
	for i, want := range Schema.Fields() {
		got := schema.Field(i)
		if got.Name != want.Name || !arrow.TypeEqual(got.Type, want.Type) {
			return fmt.Errorf("cold arrow column %d is %s %s, want %s %s", i, got.Name, got.Type, want.Name, want.Type)
		}
	}
	return nil
}

// AdoptBatch returns a record whose schema is Schema.
// Names and types must
// already match. A metadata or nullability difference is rewritten without
// copying column buffers. The caller releases the returned record.
func AdoptBatch(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(Schema) {
		rec.Retain()
		return rec, nil
	}
	if err := SchemaCompatible(rec.Schema()); err != nil {
		return nil, err
	}
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		cols[i] = rec.Column(i)
	}
	return array.NewRecordBatch(Schema, cols, rec.NumRows()), nil
}

// writeRecordBatches writes the record batches as an Arrow IPC stream in Schema.
func writeRecordBatches(writer *ipc.Writer, results model.Results) (int, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, Schema)
	defer builder.Release()

	n := 0
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		rec := builder.NewRecordBatch()
		defer rec.Release()
		pending = 0
		return writer.Write(rec)
	}
	for results.Next() {
		if err := appendRecord(builder, results.Record()); err != nil {
			return n, err
		}
		n++
		pending++
		if pending >= BatchRows {
			if err := flush(); err != nil {
				return n, err
			}
		}
	}
	if err := results.Err(); err != nil {
		return n, err
	}
	if err := flush(); err != nil {
		return n, err
	}
	return n, nil
}

// appendRecord appends the record to the builder.
func appendRecord(b *array.RecordBuilder, rec *model.Record) error {
	ts := b.Field(FieldTimestamp).(*array.TimestampBuilder)
	ts.Append(arrow.Timestamp(rec.Timestamp.UTC().UnixNano()))

	b.Field(FieldName).(*array.StringBuilder).Append(rec.Name)
	if err := appendValue(b, rec.Value); err != nil {
		return err
	}
	return appendMeta(b, rec.Meta)
}

// appendValue writes the JSON scalar for one hot or dummy record.
// Cold rows already carry this text from to_json in the DuckDB projection.
// already carry this text from to_json in the DuckDB projection.
func appendValue(b *array.RecordBuilder, v any) error {
	sb := b.Field(FieldValue).(*array.StringBuilder)
	if v == nil {
		sb.AppendNull()
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("value: %w", err)
	}
	sb.Append(string(raw))
	return nil
}

// appendMeta writes one JSON object.
// A nil map is an Arrow null. An empty
// map is {}. Keys are sorted by encoding/json, matching a stable object.
func appendMeta(b *array.RecordBuilder, meta map[string]string) error {
	sb := b.Field(FieldMeta).(*array.StringBuilder)
	if meta == nil {
		sb.AppendNull()
		return nil
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("meta: %w", err)
	}
	sb.Append(string(raw))
	return nil
}
