using System;
using System.Collections.Generic;
using System.Text;

namespace DownstreamMessages.Models
{
    public class Telemetry
    {
        public long Processed;
        public long Dupes;
        public long Gaps;
        public long Evictions;
    }
}
