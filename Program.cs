using DownstreamMessages.Messaging;
using DownstreamMessages.Models;
using System.Diagnostics;

var downstream = new Downstream();

var rnd = new Random();

var sessionAMessages = new[]
{
            new Message { SessionId="A", SequenceNumber=1, Payload="A-1" },
            new Message { SessionId="A", SequenceNumber=3, Payload="A-3 early" },
            new Message { SessionId="A", SequenceNumber=2, Payload="A-2" },
            new Message { SessionId="A", SequenceNumber=2, Payload="A-2 DUP" },
            new Message { SessionId="A", SequenceNumber=4, Payload="A-4" }
};

var sessionBMessages = new[]
{
            new Message { SessionId="B", SequenceNumber=1, Payload="B-1" },
            new Message { SessionId="B", SequenceNumber=2, Payload="B-2" },
            new Message { SessionId="B", SequenceNumber=4, Payload="B-4 gap" },
            new Message { SessionId="B", SequenceNumber=3, Payload="B-3 late" }
};

Task SendMessages(Message[] msgs) => Task.Run(async () =>
{
    foreach (var msg in msgs)
    {
        await Task.Delay(rnd.Next(50, 300)); 
        downstream.OnMessage(msg);
    }
});

await Task.WhenAll(
    SendMessages(sessionAMessages),
    SendMessages(sessionBMessages)
);
PrintTelemetry(downstream);

static void PrintTelemetry(Downstream downstream)
{
    var t = downstream.Counters;
    Console.WriteLine($"Telemetry:\n Processed={t.Processed}, Dupes={t.Dupes}, Gaps={t.Gaps}, Evictions={t.Evictions}");
}

