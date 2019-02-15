using Microsoft.Azure.EventHubs;
using static Microsoft.Azure.EventHubs.EventData;
using System;
using System.Reflection;

namespace Orleans.ServiceBus.Providers.Testing
{
    /// <summary>
    /// Setter for EventData members
    /// </summary>
    public static class EventDataProxyMethods
    {
        /// <summary>
        /// Set EventData.Offset
        /// </summary>
        /// <param name="eventData"></param>
        /// <param name="offSet"></param>
        public static void SetOffset(this EventData eventData, string offSet)
        {
            EventDataMethodCache.Instance.SetOffset(eventData, offSet);
        }

        /// <summary>
        /// Setter for EventData.SequenceNumber
        /// </summary>
        /// <param name="eventData"></param>
        /// <param name="sequenceNumber"></param>
        public static void SetSequenceNumber(this EventData eventData, long sequenceNumber)
        {
            EventDataMethodCache.Instance.SetSequenceNumber(eventData, sequenceNumber);
        }
        /// <summary>
        /// Setter for EventData.EnqueueTimeUtc
        /// </summary>
        /// <param name="eventData"></param>
        /// <param name="enqueueTime"></param>
        public static void SetEnqueuedTimeUtc(this EventData eventData, DateTime enqueueTime)
        {
            EventDataMethodCache.Instance.SetEnqueuedTimeUtc(eventData, enqueueTime);
        }

        /// <summary>
        /// Setter for EventData.PartitionKey
        /// </summary>
        /// <param name="eventData"></param>
        /// <param name="partitionKey"></param>
        public static void SetPartitionKey(this EventData eventData, string partitionKey)
        {
            EventDataMethodCache.Instance.SetPartitionKey(eventData, partitionKey);
        }
    }

    internal class EventDataMethodCache
    {
        public static EventDataMethodCache Instance = new EventDataMethodCache();
        private readonly Action<object, object> systemPropertiesSetter;
        private EventDataMethodCache()
        {
            var systemPropertiesName = nameof(EventData.SystemProperties);
            this.systemPropertiesSetter = typeof(EventData).GetProperty(systemPropertiesName).SetValue;
        }
        private void SetEmptySystemPropertiesIfNull(EventData eventData)
        {
            if (eventData.SystemProperties == null)
            {
                var emptySystemProperties = SystemPropertiesCollectionMethodCache.Instance.Create();
                this.systemPropertiesSetter(eventData, emptySystemProperties);
            }
        }
        public void SetOffset(EventData eventData, string offSet)
        {
            SetEmptySystemPropertiesIfNull(eventData);
            eventData.SystemProperties["x-opt-offset"] = offSet;
        }

        public void SetSequenceNumber(EventData eventData, long sequenceNumber)
        {
            SetEmptySystemPropertiesIfNull(eventData);
            eventData.SystemProperties["x-opt-sequence-number"] = sequenceNumber;
        }
        public void SetEnqueuedTimeUtc(EventData eventData, DateTime enqueueTime)
        {
            SetEmptySystemPropertiesIfNull(eventData);
            eventData.SystemProperties["x-opt-enqueued-time"] = enqueueTime;
        }

        public void SetPartitionKey(EventData eventData, string partitionKey)
        {
            SetEmptySystemPropertiesIfNull(eventData);
            eventData.SystemProperties["x-opt-partition-key"] = partitionKey;
        }
    }

    internal class SystemPropertiesCollectionMethodCache
    {
        public static SystemPropertiesCollectionMethodCache Instance = new SystemPropertiesCollectionMethodCache();
        private readonly ConstructorInfo zeroArgConstructorInfo;
        private SystemPropertiesCollectionMethodCache()
        {
            this.zeroArgConstructorInfo =
                typeof(SystemPropertiesCollection).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        }

        public SystemPropertiesCollection Create()
        {
            return (SystemPropertiesCollection)this.zeroArgConstructorInfo.Invoke(null);
        }
    }
}
