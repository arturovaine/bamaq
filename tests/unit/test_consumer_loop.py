from app.adapters.inbound.consumer import NOT_BEFORE_HEADER, KafkaConsumerLoop


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class FakeMsg:
    def __init__(self, topic="t", partition=0, offset=7, value=b"{}", headers=None, key=b"k"):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._headers, self._key = value, headers, key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def key(self):
        return self._key

    def error(self):
        return None


class FakeKafkaConsumer:
    def __init__(self):
        self.paused, self.resumed, self.seeks, self.commits = [], [], [], []

    def pause(self, tps):
        self.paused.extend(tps)

    def resume(self, tps):
        self.resumed.extend(tps)

    def seek(self, tp):
        self.seeks.append(tp)

    def commit(self, message, asynchronous):
        self.commits.append(message)


class FakeHandler:
    def __init__(self):
        self.handled = []

    def handle(self, *, value, headers, key):
        self.handled.append((value, headers, key))


def make_loop():
    consumer, handler, clock = FakeKafkaConsumer(), FakeHandler(), FakeClock()
    return KafkaConsumerLoop(consumer, handler, now=clock), consumer, handler, clock


def test_normal_message_is_handled_and_committed():
    loop, consumer, handler, _ = make_loop()
    msg = FakeMsg()
    loop.process_message(msg)
    assert len(handler.handled) == 1
    assert consumer.commits == [msg]


def test_premature_retry_pauses_partition_without_commit():
    loop, consumer, handler, clock = make_loop()
    msg = FakeMsg(headers=[(NOT_BEFORE_HEADER, str(clock.t + 60).encode())])
    loop.process_message(msg)
    assert handler.handled == []          # não processa antes da hora
    assert consumer.commits == []         # offset não é commitado
    assert len(consumer.paused) == 1
    assert consumer.seeks[0].offset == msg.offset()  # rebobina p/ reentrega


def test_due_retry_is_processed_normally():
    loop, consumer, handler, clock = make_loop()
    msg = FakeMsg(headers=[(NOT_BEFORE_HEADER, str(clock.t - 1).encode())])
    loop.process_message(msg)
    assert len(handler.handled) == 1
    assert consumer.commits == [msg]
    assert consumer.paused == []


def test_partition_resumes_when_due():
    loop, consumer, _, clock = make_loop()
    msg = FakeMsg(headers=[(NOT_BEFORE_HEADER, str(clock.t + 60).encode())])
    loop.process_message(msg)

    loop.resume_due_partitions()
    assert consumer.resumed == []         # ainda não chegou a hora

    clock.t += 61
    loop.resume_due_partitions()
    assert len(consumer.resumed) == 1
    assert consumer.resumed[0].topic == msg.topic()

    loop.resume_due_partitions()
    assert len(consumer.resumed) == 1     # não retoma duas vezes
