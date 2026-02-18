using System;

namespace DotNetCore.CAP.RabbitMQ;

public static class MessagingTopologyHelper
{
    private const string ReplicaFlag = ".replica.";

    public static MessagingTopology GetTopology(string groupId)
    {
        var replicaIndex = groupId.IndexOf(ReplicaFlag, StringComparison.Ordinal);
        if (replicaIndex <= -1)
        {
            return new TopicMessagingTopology
            {
                GroupName = groupId
            };
        }

        var baseGroup = groupId[..replicaIndex];
        return new ConsistentHashMessagingTopology
        {
            GroupName = baseGroup,
            QueueName = groupId,
            QueueBindingExchangeName = baseGroup.Replace(".queue.", ".exchange.")
        };
    }

    public static string GetConsistentProcessingGroupId(string groupId, string replicaId)
    {
        return $"{groupId}{ReplicaFlag}{replicaId}";
    }

    /// <summary>
    /// Extracts the base group name from a replica-suffixed group id.
    /// Returns the input unchanged if no replica suffix is present.
    /// </summary>
    public static string GetBaseGroup(string groupId)
    {
        var idx = groupId.IndexOf(ReplicaFlag, StringComparison.Ordinal);
        return idx > 0 ? groupId[..idx] : groupId;
    }
}