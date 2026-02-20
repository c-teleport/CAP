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
    private readonly IServiceProvider _serviceProvider;

    public RabbitMqConsumerClientFactory(IOptions<RabbitMQOptions> rabbitMqOptions, IConnectionChannelPool channelPool,
        IServiceProvider serviceProvider)
    {
        _rabbitMqOptions = rabbitMqOptions;
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
