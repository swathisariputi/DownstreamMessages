using System;
using System.Collections.Generic;
using System.Text;

namespace DownstreamMessages.Models
{
    public class SessionState
    {
        public object Lock { get; } = new object();
        public long NextExpected { get; set; } = 1;
        public SortedDictionary<long, Message> Buffer { get; } = new();
    }
}
