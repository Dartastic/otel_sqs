// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_sqs_suppression.dart';

const _tracerName = 'dartastic_otel_libs_sqs';
const _systemValue = 'aws_sqs';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from `tracedSqsSendMessage` callback.
class SqsSendResult<T> {
  const SqsSendResult({required this.value, this.messageId});
  final T value;
  final String? messageId;
}

/// Derives a short queue name from an SQS URL.
String _queueNameFromUrl(String queueUrl) {
  final i = queueUrl.lastIndexOf('/');
  return i < 0 ? queueUrl : queueUrl.substring(i + 1);
}

/// Wraps an SQS `SendMessage` call in a PRODUCER span using OTel
/// messaging semconv (messaging.system=aws_sqs).
Future<T> tracedSqsSendMessage<T>({
  required String queueUrl,
  required Future<SqsSendResult<T>> Function() invoke,
  int? payloadSize,
  String? region,
  String? messageGroupId,
}) async {
  if (sqsInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final queue = _queueNameFromUrl(queueUrl);
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': queue,
    'aws.sqs.queue.url': queueUrl,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (region != null) m['cloud.region'] = region;
  if (messageGroupId != null) {
    m['aws.sqs.message_group.id'] = messageGroupId;
  }

  final span = _tracer().startSpan(
    'send $queue',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.messageId != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.messageId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps an SQS `ReceiveMessage` poll in a CONSUMER span; if the
/// poll returns N messages, set [messageCount] so downstream tools
/// can compute per-message averages.
Future<T> tracedSqsReceiveMessage<T>({
  required String queueUrl,
  required Future<T> Function() invoke,
  int? messageCount,
  String? region,
}) async {
  if (sqsInstrumentationSuppressed()) {
    return invoke();
  }
  final queue = _queueNameFromUrl(queueUrl);
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'receive',
    'messaging.destination.name': queue,
    'aws.sqs.queue.url': queueUrl,
  };
  if (messageCount != null) {
    m['messaging.batch.message_count'] = messageCount;
  }
  if (region != null) m['cloud.region'] = region;

  final span = _tracer().startSpan(
    'receive $queue',
    kind: SpanKind.consumer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps an SQS `DeleteMessage` in a CLIENT span (logical ack).
Future<T> tracedSqsDeleteMessage<T>({
  required String queueUrl,
  required String receiptHandle,
  required Future<T> Function() invoke,
  String? region,
}) async {
  if (sqsInstrumentationSuppressed()) {
    return invoke();
  }
  final queue = _queueNameFromUrl(queueUrl);
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'settle',
    'messaging.destination.name': queue,
    'aws.sqs.queue.url': queueUrl,
    'aws.sqs.receipt_handle': receiptHandle,
  };
  if (region != null) m['cloud.region'] = region;

  final span = _tracer().startSpan(
    'delete $queue',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
