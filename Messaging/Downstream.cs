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
        private readonly int _maxBufferedPerSession;

        public Downstream(int maxBufferedPerSession = 100)
        {
            _maxBufferedPerSession = maxBufferedPerSession;
        }

        public void OnMessage(Message msg)
        {
            var state = _sessions.GetOrAdd(msg.SessionId, _ => new SessionState());

            lock (state)
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
                    RegisterGap(state, state.NextExpected, msg.SequenceNumber - 1);
                    state.Buffer[msg.SequenceNumber] = msg;
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
            Console.WriteLine($"Processed message: Session={msg.SessionId}, Seq={msg.SequenceNumber}, Payload={msg.Payload}");
            state.NextExpected++;
            Counters.Processed++;
            ResolveMissing(state, msg.SequenceNumber);
        }

        private void EnforceBufferLimit(SessionState state)
        {
            if (state.Buffer.Count <= _maxBufferedPerSession)
                return;

            var largestSeq = state.Buffer.Keys.Max();
            state.Buffer.Remove(largestSeq);

            Counters.Evictions++;
        }
        private void RegisterGap(SessionState state, long start, long end)
        {
            if (start > end)
                return;

            foreach (var (s, e) in state.MissingRanges)
            {
                if (start >= s && end <= e)
                {
                    return;
                }
                //else
                //{
                //    Counters.Gaps++;
                //    break;
                //}
            }

            Counters.Gaps++;
            state.MissingRanges.Add((start, end));
            MergeRanges(state.MissingRanges);
        }

        private void ResolveMissing(SessionState state, long seq)
        {
            for (int i = 0; i < state.MissingRanges.Count; i++)
            {
                var (start, end) = state.MissingRanges[i];

                if (seq < start || seq > end)
                    continue;

                state.MissingRanges.RemoveAt(i);

                if (start <= seq - 1)
                    state.MissingRanges.Add((start, seq - 1));

                if (seq + 1 <= end)
                    state.MissingRanges.Add((seq + 1, end));

                MergeRanges(state.MissingRanges);
                return;
            }
        }
        public IReadOnlyList<(long start, long end)> GetMissingRanges(string sessionId)
        {
            if (!_sessions.TryGetValue(sessionId, out var state))
                return Array.Empty<(long, long)>();

            lock (state)
            {
                foreach(var x in state.MissingRanges)
                {
                    Console.WriteLine(x);
                }
                return state.MissingRanges.ToList();
            }
        }

        private void MergeRanges(List<(long start, long end)> ranges)
        {
            if (ranges.Count <= 1)
                return;

            var ordered = ranges.OrderBy(r => r.start).ToList();
            ranges.Clear();

            var current = ordered[0];

            for (int i = 1; i < ordered.Count; i++)
            {
                var next = ordered[i];

                if (next.start <= current.end + 1)
                {
                    current.end = Math.Max(current.end, next.end);
                }
                else
                {
                    ranges.Add(current);
                    current = next;
                }
            }
            ranges.Add(current);
        }
    }
}
