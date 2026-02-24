using DownstreamMessages.Messaging;
using DownstreamMessages.Models;

var downstream = new Downstream();

var messages = new[]
{
    new Message { SessionId="A", SequenceNumber=1, Payload="First" },
    new Message { SessionId="A", SequenceNumber=3, Payload="Third (early)" },
    new Message { SessionId="A", SequenceNumber=2, Payload="Second" },
    new Message { SessionId="A", SequenceNumber=2, Payload="Duplicate Second" },
    new Message { SessionId="A", SequenceNumber=5, Payload="Fifth (gap)" },
    new Message { SessionId="A", SequenceNumber=4, Payload="Fourth" }
};

foreach (var msg in messages)
{
    downstream.OnMessage(msg);
    Console.WriteLine(downstream.GetMissingRanges(msg.SessionId));
    PrintTelemetry(downstream);
    Console.WriteLine();
}

static void PrintTelemetry(Downstream p)
{
    var t = p.Counters;
    Console.WriteLine($"Telemetry:\n Processed={t.Processed}, Duplicates={t.Duplicates}, Gaps={t.Gaps}, Eliminations={t.Eliminations}");
}