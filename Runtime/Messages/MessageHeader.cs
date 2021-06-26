using Unity.Networking.Transport;

namespace Networking
{
    public abstract class MessageHeader
    {
        private static uint nextID = 0;
        private static uint NextID => ++nextID;

        public abstract ushort Type { get; }
        public uint ID { get; private set; } = NextID;

        public virtual void SerializeObject(ref DataStreamWriter writer)
        {
            writer.WriteUShort(Type);
            writer.WriteUInt(ID);
        }

        public virtual void DeserializeObject(ref DataStreamReader reader)
        {
            // Note that Type has already been deserialized
            ID = reader.ReadUInt();
        }
    }
}
