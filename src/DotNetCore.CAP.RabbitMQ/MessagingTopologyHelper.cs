using System;

namespace DotNetCore.CAP.RabbitMQ;

public static class MessagingTopologyHelper
{
    const string ReplicaFlag = ".replica.";
    
    public static MessagingTopology GetTopology(string groupId)
    {
        var replicaIndex = groupId.IndexOf(ReplicaFlag, StringComparison.Ordinal);
        if (replicaIndex > -1)
        {
            return new MessagingTopology
            {
                QueueName = groupId,
                QueueBindingExchangeType = RabbitMQOptions.ConsistentHashExchangeType,
                QueueBindingExchangeName = groupId[..replicaIndex]
            };
        }
        
        return new MessagingTopology
        {
            QueueName = groupId,
            QueueBindingExchangeType = RabbitMQOptions.ExchangeType
        };
    }

    public static string GetConsistentProcessingGroupId(string groupId, string replicaId)
    {
        return $"{groupId}{ReplicaFlag}{replicaId}";
    }
}