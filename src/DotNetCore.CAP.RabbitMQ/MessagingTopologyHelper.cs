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
    
    /// <summary>
    /// Builds the shard-specific group id for a consistent-hash consumer by appending
    /// a replica suffix to the logical group id.
    /// </summary>
    /// <param name="groupId">
    /// The logical (base) group id shared by all replicas (e.g. <c>"myapp.queue.v1"</c>).
    /// </param>
    /// <param name="replicaId">
    /// A unique identifier for this replica/shard instance (e.g. <c>"0"</c>, <c>"pod-a"</c>).
    /// </param>
    /// <returns>
    /// A replica-suffixed group id in the form <c>"{groupId}.replica.{replicaId}"</c>
    /// (e.g. <c>"myapp.queue.v1.replica.0"</c>). This value is used as the RabbitMQ queue
    /// name for the shard and as the key in <see cref="MethodMatcherCache"/> Entries.
    /// <see cref="GetTopology"/> detects the <c>.replica.</c> marker to select
    /// <see cref="ConsistentHashMessagingTopology"/> for this group.
    /// </returns>
    public static string GetConsistentProcessingGroupId(string groupId, string replicaId)
    {
        return $"{groupId}{ReplicaFlag}{replicaId}";
    }
}