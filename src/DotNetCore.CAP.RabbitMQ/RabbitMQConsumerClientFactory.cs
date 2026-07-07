// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using DotNetCore.CAP.Transport;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMqConsumerClientFactory : IConsumerClientFactory
{
    private readonly IConnectionChannelPool _connectionChannelPool;
    private readonly IOptions<RabbitMQOptions> _rabbitMqOptions;
    private readonly CapOptions _capOptions;
    private readonly IServiceProvider _serviceProvider;

    public RabbitMqConsumerClientFactory(IOptions<RabbitMQOptions> rabbitMqOptions, IOptions<CapOptions> capOptions,
        IConnectionChannelPool channelPool, IServiceProvider serviceProvider)
    {
        _rabbitMqOptions = rabbitMqOptions;
        _capOptions = capOptions.Value;
        _connectionChannelPool = channelPool;
        _serviceProvider = serviceProvider;
    }

    public Task<IConsumerClient> CreateAsync(string groupName, byte groupConcurrent)
    {
        var topology = MessagingTopologyHelper.GetTopology(groupName);
        return topology switch
        {
            ConsistentHashMessagingTopology ch => CreateConsistentProcessingClientAsync(ch),
            TopicMessagingTopology topic => CreateConsumerClientAsync(topic, groupConcurrent),
            _ => throw new ArgumentOutOfRangeException(nameof(groupName), $"Unhandled topology type for group '{groupName}'.")
        };
    }

    private async Task<IConsumerClient> CreateConsistentProcessingClientAsync(ConsistentHashMessagingTopology topology)
    {
        if (_capOptions.EnableSubscriberParallelExecute)
        {
            throw new InvalidOperationException(
                $"{nameof(CapOptions.EnableSubscriberParallelExecute)} cannot be enabled together with a consistent-hash " +
                $"messaging topology (group '{topology.GroupName}', queue '{topology.QueueName}'). Parallel subscriber " +
                "execution buffers consumed messages and processes them concurrently across worker threads, which breaks the " +
                "per-key ordering and shard affinity that consistent-hash routing is meant to guarantee. Disable " +
                $"{nameof(CapOptions.EnableSubscriberParallelExecute)} or stop using a consistent-hash topology for this group.");
        }

        try
        {
            var client = new RabbitMqConsistentProcessingClient(topology, _connectionChannelPool, _rabbitMqOptions, _serviceProvider);

            await client.ConnectAsync();

            return client;
        }
        catch (Exception e)
        {
            throw new BrokerConnectionException(e);
        }
    }

    private async Task<IConsumerClient> CreateConsumerClientAsync(TopicMessagingTopology topology, byte concurrent)
    {
        try
        {
            var client = new RabbitMqConsumerClient(topology.GroupName, concurrent, _connectionChannelPool,
                _rabbitMqOptions, _serviceProvider);

            await client.ConnectAsync();

            return client;
        }
        catch (Exception e)
        {
            throw new BrokerConnectionException(e);
        }
    }
}
