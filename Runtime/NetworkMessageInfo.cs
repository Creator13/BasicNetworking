using System;
using System.Collections.Generic;

namespace Networking
{
    public static class NetworkMessageInfo
    {
        public static readonly Dictionary<ushort, Type> typeMap =
            new Dictionary<ushort, Type> {
                {(ushort) BuiltinMessageTypes.Ping, typeof(PingMessage)},
                {(ushort) BuiltinMessageTypes.Pong, typeof(PongMessage)}
            };
    }
}
