package distributor

import (
	"math"
	"time"

	"github.com/gogo/status"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/dataquality"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/pkg/validation"
	"github.com/segmentio/fasthash/fnv1a"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
)

// requiresLegacyTraceBatches identifies consumers that currently operate on
// request-wide tempopb batches rather than routed fragments. Keep their existing
// decode path until they have pdata-native adapters with equivalent semantics.
func (d *Distributor) requiresLegacyTraceBatches() bool {
	return d.cfg.LogReceivedSpans.Enabled ||
		d.cfg.MetricReceivedSpans.Enabled ||
		d.cfg.LogDiscardedSpans.Enabled ||
		d.usage != nil
}

// requestsByTraceIDFromPdata rebuilds the trace-owned fragments directly from
// pdata. The pdata API preserves the OTLP values that Tempo needs for traces,
// while avoiding the temporary tempopb.Trace tree produced by an OTLP wire
// round trip. The resulting fragments retain their own mutable fields, so
// attribute truncation cannot mutate pdata observed by forwarding middleware.
func requestsByTraceIDFromPdata(tracesData ptrace.Traces, userID string, spanCount, maxSpanAttrSize int) ([]uint32, []*rebatchedTrace, truncatedAttributesCount, *truncatedAttrInfo, error) {
	const tracesPerBatch = 20 // map size hint and per-trace prealloc divisor
	tracesByID := make(map[uint64]*rebatchedTrace, tracesPerBatch)
	truncatedCount := truncatedAttributesCount{}

	// Estimate spans per trace, but cap it: an uncapped spanCount/tracesPerBatch
	// would preallocate too much memory for high-cardinality requests.
	perTracePrealloc := spanCount / tracesPerBatch
	if perTracePrealloc > maxPreallocSpansPerTrace {
		perTracePrealloc = maxPreallocSpansPerTrace
	}
	if perTracePrealloc < 1 {
		perTracePrealloc = 1
	}

	var truncationExample truncatedAttrInfo
	currentTime := uint32(time.Now().Unix())
	resourceSpans := tracesData.ResourceSpans()
	for resourceIndex := 0; resourceIndex < resourceSpans.Len(); resourceIndex++ {
		resourceSpan := resourceSpans.At(resourceIndex)
		resource := pdataResourceToTempopb(resourceSpan.Resource())
		spansByILS := make(map[uint64]*v1.ScopeSpans)

		if maxSpanAttrSize > 0 {
			truncatedCount.Resource += processAttributes(resource.Attributes, maxSpanAttrSize, &truncationExample, "resource")
		}

		scopeSpans := resourceSpan.ScopeSpans()
		for scopeIndex := 0; scopeIndex < scopeSpans.Len(); scopeIndex++ {
			scopeSpan := scopeSpans.At(scopeIndex)
			scope := pdataInstrumentationScopeToTempopb(scopeSpan.Scope())

			if maxSpanAttrSize > 0 {
				truncatedCount.Scope += processAttributes(scope.Attributes, maxSpanAttrSize, &truncationExample, "scope")
			}

			spans := scopeSpan.Spans()
			for spanIndex := 0; spanIndex < spans.Len(); spanIndex++ {
				spanData := spans.At(spanIndex)
				span := pdataSpanToTempopb(spanData)

				if maxSpanAttrSize > 0 {
					truncatedCount.Span += processAttributes(span.Attributes, maxSpanAttrSize, &truncationExample, "span")
					for _, event := range span.Events {
						truncatedCount.Event += processAttributes(event.Attributes, maxSpanAttrSize, &truncationExample, "event")
					}
					for _, link := range span.Links {
						truncatedCount.Link += processAttributes(link.Attributes, maxSpanAttrSize, &truncationExample, "link")
					}
				}

				traceID := span.TraceId
				if !validation.ValidTraceID(traceID) {
					overrides.RecordDiscardedSpans(spanCount, overrides.ReasonInvalidTraceID, userID)
					return nil, nil, truncatedAttributesCount{}, nil, status.Errorf(codes.InvalidArgument, "trace ids must be 128 bit, received %d bits", len(traceID)*8)
				}
				if !validation.ValidSpanID(span.SpanId) {
					overrides.RecordDiscardedSpans(spanCount, overrides.ReasonInvalidSpanID, userID)
					return nil, nil, truncatedAttributesCount{}, nil, status.Errorf(codes.InvalidArgument, "span ids must be 64 bit and not all zero, received %d bits", len(span.SpanId)*8)
				}

				traceKey := util.HashForTraceID(traceID)
				ilsKey := fnv1a.AddString64(fnv1a.AddString64(traceKey, scope.Name), scope.Version)
				existingILS, ilsAdded := spansByILS[ilsKey]
				if !ilsAdded {
					existingILS = &v1.ScopeSpans{
						Scope: scope,
						Spans: make([]*v1.Span, 0, perTracePrealloc),
					}
					spansByILS[ilsKey] = existingILS
				}
				existingILS.Spans = append(existingILS.Spans, span)

				existingTrace, ok := tracesByID[traceKey]
				if !ok {
					existingTrace = &rebatchedTrace{
						id: traceID,
						trace: &tempopb.Trace{
							ResourceSpans: make([]*v1.ResourceSpans, 0, perTracePrealloc),
						},
						start:     math.MaxUint32,
						spanCount: 0,
					}
					tracesByID[traceKey] = existingTrace
				}

				start, end := startEndFromSpan(span)
				if existingTrace.end < end {
					existingTrace.end = end
				}
				if existingTrace.start > start {
					existingTrace.start = start
				}
				if !ilsAdded {
					existingTrace.trace.ResourceSpans = append(existingTrace.trace.ResourceSpans, &v1.ResourceSpans{
						Resource:   resource,
						ScopeSpans: []*v1.ScopeSpans{existingILS},
					})
				}
				existingTrace.spanCount++

				if end > currentTime {
					dataquality.MetricSpanInFuture.WithLabelValues(userID).Observe(float64(end - currentTime))
				} else {
					dataquality.MetricSpanInPast.WithLabelValues(userID).Observe(float64(currentTime - end))
				}
			}
		}
	}

	metricTracesPerBatch.Observe(float64(len(tracesByID)))
	ringTokens := make([]uint32, 0, len(tracesByID))
	rebatchedTraces := make([]*rebatchedTrace, 0, len(tracesByID))
	for _, trace := range tracesByID {
		ringTokens = append(ringTokens, util.TokenFor(userID, trace.id))
		rebatchedTraces = append(rebatchedTraces, trace)
	}

	if truncationExample.origSize > 0 {
		return ringTokens, rebatchedTraces, truncatedCount, &truncationExample, nil
	}
	return ringTokens, rebatchedTraces, truncatedCount, nil, nil
}

func pdataResourceToTempopb(resource pcommon.Resource) *v1_resource.Resource {
	return &v1_resource.Resource{
		Attributes:             pdataAttributesToTempopb(resource.Attributes()),
		DroppedAttributesCount: resource.DroppedAttributesCount(),
	}
}

func pdataInstrumentationScopeToTempopb(scope pcommon.InstrumentationScope) *v1_common.InstrumentationScope {
	return &v1_common.InstrumentationScope{
		Name:                   scope.Name(),
		Version:                scope.Version(),
		Attributes:             pdataAttributesToTempopb(scope.Attributes()),
		DroppedAttributesCount: scope.DroppedAttributesCount(),
	}
}

func pdataSpanToTempopb(span ptrace.Span) *v1.Span {
	return &v1.Span{
		TraceId:                pdataTraceIDToBytes(span.TraceID()),
		SpanId:                 pdataSpanIDToBytes(span.SpanID()),
		TraceState:             span.TraceState().AsRaw(),
		ParentSpanId:           pdataSpanIDToBytes(span.ParentSpanID()),
		Flags:                  span.Flags(),
		Name:                   span.Name(),
		Kind:                   v1.Span_SpanKind(span.Kind()),
		StartTimeUnixNano:      uint64(span.StartTimestamp()),
		EndTimeUnixNano:        uint64(span.EndTimestamp()),
		Attributes:             pdataAttributesToTempopb(span.Attributes()),
		DroppedAttributesCount: span.DroppedAttributesCount(),
		Events:                 pdataEventsToTempopb(span.Events()),
		DroppedEventsCount:     span.DroppedEventsCount(),
		Links:                  pdataLinksToTempopb(span.Links()),
		DroppedLinksCount:      span.DroppedLinksCount(),
		Status: &v1.Status{
			Message: span.Status().Message(),
			Code:    v1.Status_StatusCode(span.Status().Code()),
		},
	}
}

func pdataEventsToTempopb(events ptrace.SpanEventSlice) []*v1.Span_Event {
	if events.Len() == 0 {
		return nil
	}

	result := make([]*v1.Span_Event, 0, events.Len())
	for index := 0; index < events.Len(); index++ {
		event := events.At(index)
		result = append(result, &v1.Span_Event{
			TimeUnixNano:           uint64(event.Timestamp()),
			Name:                   event.Name(),
			Attributes:             pdataAttributesToTempopb(event.Attributes()),
			DroppedAttributesCount: event.DroppedAttributesCount(),
		})
	}
	return result
}

func pdataLinksToTempopb(links ptrace.SpanLinkSlice) []*v1.Span_Link {
	if links.Len() == 0 {
		return nil
	}

	result := make([]*v1.Span_Link, 0, links.Len())
	for index := 0; index < links.Len(); index++ {
		link := links.At(index)
		result = append(result, &v1.Span_Link{
			TraceId:                pdataTraceIDToBytes(link.TraceID()),
			SpanId:                 pdataSpanIDToBytes(link.SpanID()),
			TraceState:             link.TraceState().AsRaw(),
			Attributes:             pdataAttributesToTempopb(link.Attributes()),
			DroppedAttributesCount: link.DroppedAttributesCount(),
			Flags:                  link.Flags(),
		})
	}
	return result
}

func pdataAttributesToTempopb(attributes pcommon.Map) []*v1_common.KeyValue {
	if attributes.Len() == 0 {
		return nil
	}

	result := make([]*v1_common.KeyValue, 0, attributes.Len())
	attributes.Range(func(key string, value pcommon.Value) bool {
		result = append(result, &v1_common.KeyValue{
			Key:   key,
			Value: pdataValueToTempopb(value),
		})
		return true
	})
	return result
}

func pdataValueToTempopb(value pcommon.Value) *v1_common.AnyValue {
	result := &v1_common.AnyValue{}
	switch value.Type() {
	case pcommon.ValueTypeStr:
		result.Value = &v1_common.AnyValue_StringValue{StringValue: value.Str()}
	case pcommon.ValueTypeBool:
		result.Value = &v1_common.AnyValue_BoolValue{BoolValue: value.Bool()}
	case pcommon.ValueTypeInt:
		result.Value = &v1_common.AnyValue_IntValue{IntValue: value.Int()}
	case pcommon.ValueTypeDouble:
		result.Value = &v1_common.AnyValue_DoubleValue{DoubleValue: value.Double()}
	case pcommon.ValueTypeMap:
		result.Value = &v1_common.AnyValue_KvlistValue{KvlistValue: &v1_common.KeyValueList{Values: pdataAttributesToTempopb(value.Map())}}
	case pcommon.ValueTypeSlice:
		values := value.Slice()
		array := &v1_common.ArrayValue{}
		if values.Len() > 0 {
			array.Values = make([]*v1_common.AnyValue, 0, values.Len())
			for index := 0; index < values.Len(); index++ {
				array.Values = append(array.Values, pdataValueToTempopb(values.At(index)))
			}
		}
		result.Value = &v1_common.AnyValue_ArrayValue{ArrayValue: array}
	case pcommon.ValueTypeBytes:
		result.Value = &v1_common.AnyValue_BytesValue{BytesValue: append([]byte(nil), value.Bytes().AsRaw()...)}
	}
	return result
}

func pdataTraceIDToBytes(traceID pcommon.TraceID) []byte {
	return append([]byte(nil), traceID[:]...)
}

func pdataSpanIDToBytes(spanID pcommon.SpanID) []byte {
	return append([]byte(nil), spanID[:]...)
}

// pdataPayloadRequiresLegacyRebatch recognizes OTLP fields pdata retains
// internally but does not expose through its public trace API. The payload is
// always produced by ptrace.ProtoMarshaler immediately before this scan, so its
// wire shape is valid and no external-wire error handling belongs on this hot
// path.
func pdataPayloadRequiresLegacyRebatch(payload []byte) bool {
	for len(payload) > 0 {
		fieldNumber, fieldPayload, rest := nextPdataWireField(payload)
		payload = rest
		if fieldNumber != 1 {
			continue
		}

		resourceSpans, _ := protowire.ConsumeBytes(fieldPayload)
		if wirePayloadRequiresLegacyRebatch(resourceSpans, otlpWireResourceSpans) {
			return true
		}
	}
	return false
}

type otlpWireMessage uint8

const (
	otlpWireResourceSpans otlpWireMessage = iota
	otlpWireResource
	otlpWireScopeSpans
	otlpWireInstrumentationScope
	otlpWireSpan
	otlpWireEvent
	otlpWireLink
	otlpWireKeyValue
	otlpWireAnyValue
	otlpWireArray
	otlpWireKeyValueList
)

func wirePayloadRequiresLegacyRebatch(payload []byte, message otlpWireMessage) bool {
	for len(payload) > 0 {
		fieldNumber, fieldPayload, rest := nextPdataWireField(payload)
		payload = rest

		if message == otlpWireResource && fieldNumber == 3 {
			// pcommon.Resource has no accessor for EntityRefs.
			return true
		}
		if message == otlpWireKeyValue && fieldNumber == 3 {
			// pcommon.Map exposes Key but not KeyStrindex.
			return true
		}
		if message == otlpWireAnyValue && fieldNumber == 8 {
			// pcommon.Value has no accessor for StringValueStrindex.
			return true
		}
		if child, ok := otlpWireChild(message, fieldNumber); ok {
			value, _ := protowire.ConsumeBytes(fieldPayload)
			if wirePayloadRequiresLegacyRebatch(value, child) {
				return true
			}
		}
	}
	return false
}

func otlpWireChild(message otlpWireMessage, fieldNumber protowire.Number) (otlpWireMessage, bool) {
	switch message {
	case otlpWireResourceSpans:
		switch fieldNumber {
		case 1:
			return otlpWireResource, true
		case 2:
			return otlpWireScopeSpans, true
		}
	case otlpWireResource:
		return otlpWireKeyValue, fieldNumber == 1
	case otlpWireScopeSpans:
		switch fieldNumber {
		case 1:
			return otlpWireInstrumentationScope, true
		case 2:
			return otlpWireSpan, true
		}
	case otlpWireInstrumentationScope:
		return otlpWireKeyValue, fieldNumber == 3
	case otlpWireSpan:
		switch fieldNumber {
		case 9:
			return otlpWireKeyValue, true
		case 11:
			return otlpWireEvent, true
		case 13:
			return otlpWireLink, true
		}
	case otlpWireEvent:
		return otlpWireKeyValue, fieldNumber == 3
	case otlpWireLink:
		return otlpWireKeyValue, fieldNumber == 4
	case otlpWireKeyValue:
		return otlpWireAnyValue, fieldNumber == 2
	case otlpWireAnyValue:
		switch fieldNumber {
		case 5:
			return otlpWireArray, true
		case 6:
			return otlpWireKeyValueList, true
		}
	case otlpWireArray:
		return otlpWireAnyValue, fieldNumber == 1
	case otlpWireKeyValueList:
		return otlpWireKeyValue, fieldNumber == 1
	}
	return 0, false
}

func nextPdataWireField(payload []byte) (fieldNumber protowire.Number, fieldPayload, rest []byte) {
	fieldNumber, fieldType, tagSize := protowire.ConsumeTag(payload)
	fieldPayload = payload[tagSize:]
	fieldSize := protowire.ConsumeFieldValue(fieldNumber, fieldType, fieldPayload)
	return fieldNumber, fieldPayload, fieldPayload[fieldSize:]
}
