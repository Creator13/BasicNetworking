namespace Networking
{
    public class PongMessage : MessageHeader
    {
        public override ushort Type => (ushort) BuiltinMessageTypes.Pong;
    }

    public class PingMessage : MessageHeader
    {
        public override ushort Type => (ushort) BuiltinMessageTypes.Ping;
    }
}
