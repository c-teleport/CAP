// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using DotNetCore.CAP.Transport;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMqConsumerClientFactory : IConsumerClientFactory
{
    private readonly IConnectionChannelPool _connectionChannelPool;
    private readonly IOptions<RabbitMQOptions> _rabbitMqOptions;
    private readonly IServiceProvider _serviceProvider;

    public RabbitMqConsumerClientFactory(
        IOptions<RabbitMQOptions> rabbitMqOptions, 
        IConnectionChannelPool channelPool,
        IServiceProvider serviceProvider)
    {
        _rabbitMqOptions = rabbitMqOptions;
        _connectionChannelPool = channelPool;
        _serviceProvider = serviceProvider;
    }

    public IConsumerClient Create(string groupId, byte concurrent)
    {
        var messagingTopology = MessagingTopologyHelper.GetTopology(groupId);
        return messagingTopology.QueueBindingExchangeType switch
        {
            RabbitMQOptions.ConsistentHashExchangeType => CreateConsistentHashClient(messagingTopology),
            _ => CreateConsumerClient(messagingTopology, concurrent)
        };
    }

    private RabbitMqConsistentProcessingClient CreateConsistentHashClient(MessagingTopology topology)
    {
        try
        {
            var client = new RabbitMqConsistentProcessingClient(topology.QueueName, topology.QueueBindingExchangeName,
                _connectionChannelPool, _rabbitMqOptions, _serviceProvider);
            client.Connect().GetAwaiter().GetResult();
            return client;
        }
        catch (Exception e)
        {
            throw new BrokerConnectionException(e);
        }
    }
    
    private RabbitMqConsumerClient CreateConsumerClient(MessagingTopology topology, byte concurrent)
    {
        try
        {
            var client = new RabbitMqConsumerClient(topology.QueueName, concurrent, _connectionChannelPool,
                _rabbitMqOptions, _serviceProvider);
            client.Connect().GetAwaiter().GetResult();
            
            return client;
        }
        catch (Exception e)
        {
            throw new BrokerConnectionException(e);
        }
    }
}