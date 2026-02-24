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
                    Counters.Duplicates++;
                    return;
                }

                if (state.Buffer.ContainsKey(msg.SequenceNumber))
                {
                    Counters.Duplicates++;
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
            Console.WriteLine($"Processed message: Session={msg.SessionId}, Seq={msg.SequenceNumber}, Payload={msg.Payload}");
            state.NextExpected++;
            Counters.Processed++;
        }

        private void EnforceBufferLimit(SessionState state)
        {
            if (state.Buffer.Count <= _maxBufferedPerSession)
                return;

            var largestSeq = GetLargestKey(state.Buffer);
            state.Buffer.Remove(largestSeq);

            Counters.Eliminations++;
        }

        private static long GetLargestKey(SortedDictionary<long, Message> dict)
        {
            long last = 0;

            foreach (var key in dict.Keys)
                last = key;

            return last;
        }

        public string GetMissingRanges(string sessionId)
        {
            if (!_sessions.TryGetValue(sessionId, out var state))
                return "No Session Found";
            bool isMissed = false;

            lock (state.Lock)
            {
                var ranges = "Missed ranges: ";

                long expected = state.NextExpected;

                foreach (var seq in state.Buffer.Keys)
                {
                    if (seq > expected)
                    {
                        ranges += "(" + expected.ToString() + ", " + (seq - 1).ToString() + ")";
                        isMissed = true;
                    }

                    expected = seq + 1;
                }
                if (!isMissed) return "No missed range";
                return ranges;
            }
        }
    }
}
