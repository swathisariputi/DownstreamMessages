using DownstreamMessages.Contracts;
using DownstreamMessages.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace DownstreamMessages.Messaging
{
    public class Downstream : IDownstream
    {
        private readonly ConcurrentDictionary<string, SessionState> _sessions = new();
        public Telemetry Counters { get; } = new();
        private readonly int _maxBufferedPerSession = 3;

        public void OnMessage(Message msg)
        {
            var state = _sessions.GetOrAdd(msg.SessionId, _ => new SessionState());

            lock (state.Lock)
            {
                if (msg.SequenceNumber < state.NextExpected)
                {
                    Counters.Dupes++;
                    return;
                }

                if (state.Buffer.ContainsKey(msg.SequenceNumber))
                {
                    Counters.Dupes++;
                    return;
                }

                if (msg.SequenceNumber > state.NextExpected)
                {
                    state.Buffer[msg.SequenceNumber] = msg;
                    Counters.Gaps++;

                    EnforceBufferLimit(state);
                    return;
                }

                ProcessAndAdvance(state, msg);

                while (state.Buffer.TryGetValue(state.NextExpected, out var next))
                {
                    state.Buffer.Remove(state.NextExpected);
                    ProcessAndAdvance(state, next);
                }
            }
        }

        private void ProcessAndAdvance(SessionState state, Message msg)
        {
            OnMessage(msg);
            state.NextExpected++;
            Counters.Processed++;
        }

        private void EnforceBufferLimit(SessionState state)
        {
            if (state.Buffer.Count <= _maxBufferedPerSession)
                return;

            var largestSeq = GetLargestKey(state.Buffer);
            state.Buffer.Remove(largestSeq);

            Counters.Evictions++;
        }

        private static long GetLargestKey(SortedDictionary<long, Message> dict)
        {
            long last = 0;

            foreach (var key in dict.Keys)
                last = key;

            return last;
        }

        public IReadOnlyList<(long from, long to)> GetMissingRanges(string sessionId)
        {
            if (!_sessions.TryGetValue(sessionId, out var state))
                return [];

            lock (state.Lock)
            {
                var ranges = new List<(long, long)>();

                long expected = state.NextExpected;

                foreach (var seq in state.Buffer.Keys)
                {
                    if (seq > expected)
                    {
                        ranges.Add((expected, seq - 1));
                    }

                    expected = seq + 1;
                }

                return ranges;
            }
        }
    }
}
