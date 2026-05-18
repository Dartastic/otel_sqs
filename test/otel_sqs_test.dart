// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_sqs/otel_sqs.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_sqs-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  const queueUrl = 'https://sqs.us-east-1.amazonaws.com/123/orders-q';

  test('SendMessage records PRODUCER span with short queue name', () async {
    await tracedSqsSendMessage<String>(
      queueUrl: queueUrl,
      payloadSize: 256,
      region: 'us-east-1',
      messageGroupId: 'user-1',
      invoke: () async => const SqsSendResult(value: 'ok', messageId: 'm-1'),
    );
    final span = spans.findSpanByName('send orders-q');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'aws_sqs');
    expect(a['messaging.destination.name'], 'orders-q');
    expect(a['aws.sqs.queue.url'], queueUrl);
    expect(a['messaging.message.body.size'], 256);
    expect(a['aws.sqs.message_group.id'], 'user-1');
    expect(a['messaging.message.id'], 'm-1');
  });

  test('ReceiveMessage records CONSUMER span with batch count', () async {
    await tracedSqsReceiveMessage<String>(
      queueUrl: queueUrl,
      messageCount: 5,
      invoke: () async => 'received',
    );
    final span = spans.findSpanByName('receive orders-q');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.consumer);
    final a = attrsOf(span);
    expect(a['messaging.operation'], 'receive');
    expect(a['messaging.batch.message_count'], 5);
  });

  test('DeleteMessage records CLIENT span with receipt handle', () async {
    await tracedSqsDeleteMessage<String>(
      queueUrl: queueUrl,
      receiptHandle: 'rh-xyz',
      invoke: () async => 'ok',
    );
    final span = spans.findSpanByName('delete orders-q');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.client);
    final a = attrsOf(span);
    expect(a['messaging.operation'], 'settle');
    expect(a['aws.sqs.receipt_handle'], 'rh-xyz');
  });

  test('records exception', () async {
    await expectLater(
      tracedSqsSendMessage<String>(
        queueUrl: queueUrl,
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(
      spans.findSpanByName('send orders-q')!.status,
      SpanStatusCode.Error,
    );
  });

  test('suppression skips spans', () async {
    await runWithoutSqsInstrumentationAsync(() async {
      await tracedSqsSendMessage<String>(
        queueUrl: queueUrl,
        invoke: () async => const SqsSendResult(value: 'v'),
      );
      await tracedSqsReceiveMessage<String>(
        queueUrl: queueUrl,
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('send '), isEmpty);
    expect(spans.findSpansStartingWith('receive '), isEmpty);
  });
}
