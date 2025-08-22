// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Transport;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMqConsistentProcessingClient : IConsumerClient
{
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly IConnectionChannelPool _connectionChannelPool;
    private readonly IServiceProvider _serviceProvider;
    private readonly string _exchangeName;
    private readonly string _queueName;
    private readonly RabbitMQOptions _rabbitMqOptions;
    private RabbitMqBasicConsumer? _consumer = null;
    private IModel? _channel;
    private readonly string _queueBindingExchangeName;

    public RabbitMqConsistentProcessingClient(string queueName, 
        string queueBindingExchange,
        IConnectionChannelPool connectionChannelPool,
        IOptions<RabbitMQOptions> options,
        IServiceProvider serviceProvider)
    {
        _queueName = queueName;
        _connectionChannelPool = connectionChannelPool;
        _serviceProvider = serviceProvider;
        _rabbitMqOptions = options.Value;
        _exchangeName = connectionChannelPool.Exchange;
        _queueBindingExchangeName = queueBindingExchange;
    }

    public Func<TransportMessage, object?, Task>? OnMessageCallback { get; set; }

    public Action<LogMessageEventArgs>? OnLogCallback { get; set; }

    public BrokerAddress BrokerAddress => new("rabbitmq", $"{_rabbitMqOptions.HostName}:{_rabbitMqOptions.Port}");

    public async Task SubscribeAsync(IEnumerable<string> topics)
    {
        if (topics == null) throw new ArgumentNullException(nameof(topics));

        await ConnectAsync();

        foreach (var topic in topics)
        {
            _channel!.ExchangeBind(_queueBindingExchangeName, _exchangeName, topic);
        }
        
        const string weightOfTheShard = "1"; // In this case, all messages go to all consumers sharded equally.
        _channel!.QueueBind(_queueName, _queueBindingExchangeName, weightOfTheShard);
    }

    public async Task ListeningAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        await ConnectAsync();

        if (_rabbitMqOptions.BasicQosOptions != null)
        {
            _channel!.BasicQos(0, _rabbitMqOptions.BasicQosOptions.PrefetchCount, _rabbitMqOptions.BasicQosOptions.Global);
        }
        else
        {
            _channel!.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
        }

        _consumer = new RabbitMqBasicConsumer(_channel!, concurrent: 0, _queueName, OnMessageCallback!, OnLogCallback!,
            _rabbitMqOptions.CustomHeadersBuilder, _serviceProvider);

        try
        {
            _channel!.BasicConsume(_queueName, false, _consumer);
        }
        catch (TimeoutException ex)
        {
            await _consumer.HandleModelShutdown(null!, new ShutdownEventArgs(ShutdownInitiator.Application, 0, ex.Message + "-->" + nameof(_channel.BasicConsume)));
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.WaitHandle.WaitOne(timeout);
        }

        // ReSharper disable once FunctionNeverReturns
    }

    public Task CommitAsync(object? sender)
    {
        _consumer!.BasicAck((ulong)sender!);
        return Task.CompletedTask;
    }

    public Task RejectAsync(object? sender)
    {
        _consumer!.BasicReject((ulong)sender!);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _channel?.Dispose();
        //The connection should not be closed here, because the connection is still in use elsewhere. 
        //_connection?.Dispose();
        return ValueTask.CompletedTask;
    }

    public async Task ConnectAsync()
    {
        var connection = _connectionChannelPool.GetConnection();

        await _semaphore.WaitAsync();

        if (_channel == null || _channel.IsClosed)
        {
            _channel = connection.CreateModel();
            
            _channel.ExchangeDeclare(_exchangeName, RabbitMQOptions.ExchangeType, true);
            _channel.ExchangeDeclare(_queueBindingExchangeName,
                RabbitMQOptions.ConsistentHashExchangeType,
                true, arguments: new Dictionary<string, object?>
                {
                    { "hash-header", RabbitMQOptions.ConsistentHashHeader }
                });

            var arguments = new Dictionary<string, object?>
            {
                { "x-message-ttl", _rabbitMqOptions.QueueArguments.MessageTTL }
            };

            if (!string.IsNullOrEmpty(_rabbitMqOptions.QueueArguments.QueueMode))
                arguments.Add("x-queue-mode", _rabbitMqOptions.QueueArguments.QueueMode);

            if (!string.IsNullOrEmpty(_rabbitMqOptions.QueueArguments.QueueType))
                arguments.Add("x-queue-type", _rabbitMqOptions.QueueArguments.QueueType);

            try
            {
                _channel.QueueDeclare(_queueName, _rabbitMqOptions.QueueOptions.Durable, _rabbitMqOptions.QueueOptions.Exclusive, _rabbitMqOptions.QueueOptions.AutoDelete, arguments);
            }
            catch (TimeoutException ex)
            {
                var args = new LogMessageEventArgs
                {
                    LogType = MqLogType.ConsumerShutdown,
                    Reason = ex.Message + "-->" + nameof(_channel.QueueDeclare)
                };

                OnLogCallback!(args);
            }
        }

        _semaphore.Release();
    }
}