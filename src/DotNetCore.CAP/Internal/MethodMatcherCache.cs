// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace DotNetCore.CAP.Internal;

public class MethodMatcherCache
{
    private readonly IConsumerServiceSelector _selector;

    public MethodMatcherCache(IConsumerServiceSelector selector)
    {
        _selector = selector;
        Entries = new ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>>();
        BaseGroupEntries = new ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>>();
        GroupConcurrent = new ConcurrentDictionary<string, byte>();
    }

    private ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>> Entries { get; }

    // Secondary index: maps logical group name → shard descriptors for O(1) DB-retry fallback.
    // Populated only for descriptors where LogicalGroupName is set (i.e. sharded/replica groups).
    private ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>> BaseGroupEntries { get; }

    private ConcurrentDictionary<string, byte> GroupConcurrent { get; }

    /// <summary>
    /// Get a dictionary of candidates.In the dictionary,
    /// the Key is the CAPSubscribeAttribute Group, the Value for the current Group of candidates
    /// </summary>
    public ConcurrentDictionary<string, IReadOnlyList<ConsumerExecutorDescriptor>> GetCandidatesMethodsOfGroupNameGrouped()
    {
        if (!Entries.IsEmpty) return Entries;

        var executorCollection = _selector.SelectCandidates();

        foreach (var executor in executorCollection)
        {
            GroupConcurrent.AddOrUpdate(executor.Attribute.Group, executor.Attribute.GroupConcurrent,
                (group, val) => (byte)(val + executor.Attribute.GroupConcurrent));
        }

        var groupedCandidates = executorCollection.GroupBy(x => x.Attribute.Group);

        foreach (var item in groupedCandidates)
        {
            var descriptors = item.ToList();
            Entries.TryAdd(item.Key, descriptors);

            var baseGroup = descriptors[0].LogicalGroupName;
            if (baseGroup != null)
                BaseGroupEntries.TryAdd(baseGroup, descriptors);
        }

        return Entries;
    }

    public byte GetGroupConcurrentLimit(string group)
    {
        return GroupConcurrent.GetValueOrDefault(group, (byte)1);
    }

    /// <summary>
    /// Attempts to get the topic executor associated with the specified topic name and group name from the
    /// <see cref="Entries" />.
    /// </summary>
    /// <param name="topicName">The topic name of the value to get.</param>
    /// <param name="groupName">The group name of the value to get.</param>
    /// <param name="matchTopic">topic executor of the value.</param>
    /// <returns>true if the key was found, otherwise false. </returns>
    public bool TryGetTopicExecutor(string topicName, string groupName,
        [NotNullWhen(true)] out ConsumerExecutorDescriptor? matchTopic)
    {
        matchTopic = null;

        // Exact match — normal path for both standard and sharded consumers.
        if (Entries.TryGetValue(groupName, out var groupMatchTopics))
        {
            matchTopic = _selector.SelectBestCandidate(topicName, groupMatchTopics);
            if (matchTopic != null)
                return true;
        }

        // fallback for sharded consumer groups: when a shard consumer writes a message to DB
        // it uses the logical group name as the group header, so retried messages arrive here
        // with the logical group name rather than the shard-specific group name.
        if (BaseGroupEntries.TryGetValue(groupName, out var baseGroupMatchTopics))
        {
            matchTopic = _selector.SelectBestCandidate(topicName, baseGroupMatchTopics);
            return matchTopic != null;
        }

        return false;
    }
}