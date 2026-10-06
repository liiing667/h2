from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.stream


class TestPriority:
    """
    Basic priority tests.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    example_request_headers_bytes = [
        (b":authority", b"example.com"),
        (b":path", b"/"),
        (b":scheme", b"https"),
        (b":method", b"GET"),
    ]
    example_response_headers = [
        (":status", "200"),
        ("server", "pytest-h2"),
    ]
    server_config = h2.config.H2Configuration(client_side=False)

    def test_receiving_priority_emits_priority_update(self, frame_factory) -> None:
        """
        Receiving a priority frame emits a PriorityUpdated event.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_priority_frame(
            stream_id=1,
            weight=255,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert not c.data_to_send()

        event = events[0]
        assert isinstance(event, h2.events.PriorityUpdated)
        assert event.stream_id == 1
        assert event.depends_on == 0
        assert event.weight == 256
        assert event.exclusive is False

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_headers_with_priority_info(self, frame_factory, request_headers) -> None:
        """
        Receiving a HEADERS frame with priority information on it emits a
        PriorityUpdated event.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            headers=request_headers,
            stream_id=3,
            flags=["PRIORITY"],
            stream_weight=15,
            depends_on=1,
            exclusive=True,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        assert not c.data_to_send()

        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdated)
        assert event.stream_id == 3
        assert event.depends_on == 1
        assert event.weight == 16
        assert event.exclusive is True

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_streams_may_not_depend_on_themselves(self, frame_factory, request_headers) -> None:
        """
        A stream adjusted to depend on itself causes a Protocol Error.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            headers=request_headers,
            stream_id=3,
            flags=["PRIORITY"],
            stream_weight=15,
            depends_on=1,
            exclusive=True,
        )
        c.receive_data(f.serialize())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_priority_frame(
            stream_id=3,
            depends_on=3,
            weight=15,
        )
        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(f.serialize())

        expected_frame = frame_factory.build_goaway_frame(
            last_stream_id=3,
            error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.data_to_send() == expected_frame.serialize()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize(
        ("depends_on", "weight", "exclusive"),
        [
            (0, 256, False),
            (3, 128, False),
            (3, 128, True),
        ],
    )
    def test_can_prioritize_stream(self, depends_on, weight, exclusive,
                                   frame_factory, request_headers) -> None:
        """
        hyper-h2 can emit priority frames.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()

        c.send_headers(headers=request_headers, stream_id=1)
        c.send_headers(headers=request_headers, stream_id=3)
        c.clear_outbound_data_buffer()

        c.prioritize(
            stream_id=1,
            depends_on=depends_on,
            weight=weight,
            exclusive=exclusive,
        )

        f = frame_factory.build_priority_frame(
            stream_id=1,
            weight=weight - 1,
            depends_on=depends_on,
            exclusive=exclusive,
        )
        assert c.data_to_send() == f.serialize()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize(
        ("depends_on", "weight", "exclusive"),
        [
            (0, 256, False),
            (1, 128, False),
            (1, 128, True),
        ],
    )
    def test_emit_headers_with_priority_info(self, depends_on, weight,
                                             exclusive, frame_factory, request_headers) -> None:
        """
        It is possible to send a headers frame with priority information on
        it.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        c.send_headers(
            headers=request_headers,
            stream_id=3,
            priority_weight=weight,
            priority_depends_on=depends_on,
            priority_exclusive=exclusive,
        )

        f = frame_factory.build_headers_frame(
            headers=request_headers,
            stream_id=3,
            flags=["PRIORITY"],
            stream_weight=weight - 1,
            depends_on=depends_on,
            exclusive=exclusive,
        )
        assert c.data_to_send() == f.serialize()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_may_not_prioritize_stream_to_depend_on_self(self, frame_factory, request_headers) -> None:
        """
        A stream adjusted to depend on itself causes a Protocol Error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.send_headers(
            headers=request_headers,
            stream_id=3,
            priority_weight=255,
            priority_depends_on=0,
            priority_exclusive=False,
        )
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.prioritize(
                stream_id=3,
                depends_on=3,
            )

        assert not c.data_to_send()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_may_not_initially_set_stream_depend_on_self(self, frame_factory, request_headers) -> None:
        """
        A stream that starts by depending on itself causes a Protocol Error.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_headers(
                headers=request_headers,
                stream_id=3,
                priority_depends_on=3,
            )

        assert not c.data_to_send()

    @pytest.mark.parametrize("weight", [0, -15, 257])
    def test_prioritize_requires_valid_weight(self, weight) -> None:
        """
        A call to prioritize with an invalid weight causes a ProtocolError.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.prioritize(stream_id=1, weight=weight)

        assert not c.data_to_send()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize("weight", [0, -15, 257])
    def test_send_headers_requires_valid_weight(self, weight, request_headers) -> None:
        """
        A call to send_headers with an invalid weight causes a ProtocolError.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.send_headers(
                stream_id=1,
                headers=request_headers,
                priority_weight=weight,
            )

        assert not c.data_to_send()

    def test_prioritize_defaults(self, frame_factory) -> None:
        """
        When prioritize() is called with no explicit arguments, it emits a
        weight of 16, depending on stream zero non-exclusively.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        c.prioritize(stream_id=1)

        f = frame_factory.build_priority_frame(
            stream_id=1,
            weight=15,
            depends_on=0,
            exclusive=False,
        )
        assert c.data_to_send() == f.serialize()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize(
        "priority_kwargs",
        [
            {"priority_weight": 16},
            {"priority_depends_on": 0},
            {"priority_exclusive": False},
        ],
    )
    def test_send_headers_defaults(self, priority_kwargs, frame_factory, request_headers) -> None:
        """
        When send_headers() is called with only one explicit argument, it emits
        default values for everything else.
        """
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.clear_outbound_data_buffer()

        c.send_headers(
            stream_id=1,
            headers=request_headers,
            **priority_kwargs,
        )

        f = frame_factory.build_headers_frame(
            headers=request_headers,
            stream_id=1,
            flags=["PRIORITY"],
            stream_weight=15,
            depends_on=0,
            exclusive=False,
        )
        assert c.data_to_send() == f.serialize()

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_servers_cannot_prioritize(self, frame_factory, request_headers) -> None:
        """
        Server stacks are not allowed to call ``prioritize()``.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            stream_id=1,
            headers=request_headers,
        )
        c.receive_data(f.serialize())

        with pytest.raises(h2.exceptions.RFC1122Error):
            c.prioritize(stream_id=1)

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_servers_cannot_prioritize_with_headers(self, frame_factory, request_headers) -> None:
        """
        Server stacks are not allowed to prioritize on headers either.
        """
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            stream_id=1,
            headers=request_headers,
        )
        c.receive_data(f.serialize())

        with pytest.raises(h2.exceptions.RFC1122Error):
            c.send_headers(
                stream_id=1,
                headers=self.example_response_headers,
                priority_weight=16,
            )


class TestRFC9218PriorityHeader:
    """
    Tests for the RFC 9218 ``Priority`` header field, which is surfaced as
    the structured ``PriorityUpdateReceived`` event.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    example_request_headers_bytes = [
        (b":authority", b"example.com"),
        (b":path", b"/"),
        (b":scheme", b"https"),
        (b":method", b"GET"),
    ]
    example_response_headers = [
        (":status", "200"),
        ("server", "pytest-h2"),
    ]
    server_config = h2.config.H2Configuration(client_side=False)

    def _server_connection(self, frame_factory):
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()
        return c

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize(("priority_value", "urgency", "incremental"), [
        ("u=5", 5, False),
        ("u=1, i", 1, True),
        ("u=3, i", 3, True),
        ("u=0", 0, False),
        ("u=7", 7, False),
        ("i", 3, True),
        ("u=2", 2, False),
        ("u=6,i", 6, True),
        ("u=4, unknown=1, x", 4, False),
    ])
    def test_priority_header_emits_priority_update_received(
        self, frame_factory, request_headers, priority_value, urgency, incremental,
    ) -> None:
        """
        A request with a valid RFC 9218 Priority header field emits a
        PriorityUpdateReceived event alongside RequestReceived.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [("priority", priority_value)],
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        assert isinstance(events[0], h2.events.RequestReceived)
        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.stream_id == 1
        assert event.urgency == urgency
        assert event.incremental is incremental

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_no_priority_header_no_event(self, frame_factory, request_headers) -> None:
        """
        A request without a Priority header field emits no
        PriorityUpdateReceived event.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers,
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 1
        assert isinstance(events[0], h2.events.RequestReceived)

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_multiple_priority_headers_last_one_wins(self, frame_factory, request_headers) -> None:
        """
        When several Priority header fields are present, only the final one
        is considered.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [
                ("priority", "u=0"),
                ("priority", "u=6, i"),
            ],
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 6
        assert event.incremental is True

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_invalid_first_priority_header_ignored(self, frame_factory, request_headers) -> None:
        """
        An invalid Priority header field is ignored when a later, valid one
        is present.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [
                ("priority", "u=9"),
                ("priority", "u=2"),
            ],
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 2
        assert event.incremental is False

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    @pytest.mark.parametrize(("priority_value", "parameter"), [
        ("u=8", "u"),
        ("u=42", "u"),
        ("u=-1", "u"),
        ("u=", "u"),
        ("u", "u"),
        ("u=x", "u"),
        ("u=3.5", "u"),
        ("u=1, u=2", "u"),
        ("i=?1", "i"),
        ("i=1", "i"),
        ("i, i", "i"),
    ])
    def test_invalid_priority_header_is_protocol_error(
        self, frame_factory, request_headers, priority_value, parameter,
    ) -> None:
        """
        Malformed Priority header fields are protocol errors that name the
        offending parameter.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [("priority", priority_value)],
            stream_id=1,
        )
        with pytest.raises(h2.exceptions.ProtocolError) as exc_info:
            c.receive_data(f.serialize())

        assert f"'{parameter}'" in str(exc_info.value)

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_priority_header_with_end_stream(self, frame_factory, request_headers) -> None:
        """
        The PriorityUpdateReceived event is emitted in addition to the
        RequestReceived and StreamEnded events.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [("priority", "u=1, i")],
            stream_id=1,
            flags=["END_STREAM"],
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 3
        assert isinstance(events[0], h2.events.RequestReceived)
        assert isinstance(events[1], h2.events.StreamEnded)
        event = events[2]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 1
        assert event.incremental is True

    def test_priority_header_on_response(self, frame_factory) -> None:
        """
        The client side emits an identical PriorityUpdateReceived event when
        a response carries a Priority header field.
        """
        c = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True),
        )
        c.initiate_connection()
        c.send_headers(stream_id=1, headers=self.example_request_headers)
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            headers=self.example_response_headers + [("priority", "u=2, i")],
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        assert isinstance(events[0], h2.events.ResponseReceived)
        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.stream_id == 1
        assert event.urgency == 2
        assert event.incremental is True

    @pytest.mark.parametrize("request_headers", [example_request_headers, example_request_headers_bytes])
    def test_priority_frame_unaffected_by_priority_header(
        self, frame_factory, request_headers,
    ) -> None:
        """
        The RFC 7540 PRIORITY frame still emits PriorityUpdated with its
        original fields, independently of the RFC 9218 header field.
        """
        c = self._server_connection(frame_factory)

        f = frame_factory.build_headers_frame(
            headers=request_headers + [("priority", "u=5")],
            stream_id=3,
            flags=["PRIORITY"],
            stream_weight=15,
            depends_on=1,
            exclusive=True,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 3
        assert isinstance(events[0], h2.events.RequestReceived)

        priority_update_received = events[1]
        assert isinstance(priority_update_received, h2.events.PriorityUpdateReceived)
        assert priority_update_received.stream_id == 3
        assert priority_update_received.urgency == 5
        assert priority_update_received.incremental is False

        priority_updated = events[2]
        assert isinstance(priority_updated, h2.events.PriorityUpdated)
        assert priority_updated.stream_id == 3
        assert priority_updated.depends_on == 1
        assert priority_updated.weight == 16
        assert priority_updated.exclusive is True

    def test_priority_header_with_header_encoding(self, frame_factory) -> None:
        """
        The PriorityUpdateReceived event is also emitted when the connection
        decodes headers to unicode strings.
        """
        config = h2.config.H2Configuration(
            client_side=False, header_encoding="utf-8",
        )
        c = h2.connection.H2Connection(config=config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.clear_outbound_data_buffer()

        f = frame_factory.build_headers_frame(
            headers=self.example_request_headers + [("priority", "u=1, i")],
            stream_id=1,
        )
        events = c.receive_data(f.serialize())

        assert len(events) == 2
        event = events[1]
        assert isinstance(event, h2.events.PriorityUpdateReceived)
        assert event.urgency == 1
        assert event.incremental is True
