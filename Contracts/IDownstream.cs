using DownstreamMessages.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace DownstreamMessages.Contracts
{
    public interface IDownstream
    {
        void OnMessage(Message msg);
    }
}
