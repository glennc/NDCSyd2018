using System;

namespace BackgroundQueueProcessor
{
    internal class QueueHandlerAttribute : Attribute
    {
        public string QueueName { get; set; }

        public QueueHandlerAttribute(string v)
        {
            this.QueueName = v;
        }
    }
}