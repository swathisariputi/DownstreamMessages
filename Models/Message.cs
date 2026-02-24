using System;
using System.Collections.Generic;
using System.Text;

namespace DownstreamMessages.Models
{
    public class Message
    {
        public string SessionId { get; set; } = string.Empty;
        public long SequenceNumber { get; set; }
        public string Payload { get; set; } = string.Empty;
    }
}
