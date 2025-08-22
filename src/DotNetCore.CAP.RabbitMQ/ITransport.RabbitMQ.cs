// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Transport;
using Microsoft.Extensions.Logging;
using Rabbit = RabbitMQ.Client;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMqTransport : ITransport
{
    private readonly IConnectionChannelPool _connectionChannelPool;
    private readonly string _exchange;
    private readonly ILogger _logger;

    public RabbitMqTransport(ILogger<RabbitMqTransport> logger, IConnectionChannelPool connectionChannelPool)
    {
        _logger = logger;
        _connectionChannelPool = connectionChannelPool;
        _exchange = _connectionChannelPool.Exchange;
    }

    public BrokerAddress BrokerAddress => new("RabbitMQ", _connectionChannelPool.HostAddress);

    public async Task<OperateResult> SendAsync(TransportMessage message)
    {
        Rabbit.IModel? channel = null;
        try
        {
            channel = await _connectionChannelPool.Rent();

            var props = channel.CreateBasicProperties();
            props.DeliveryMode = 2;
            props.Headers = message.Headers.ToDictionary(x => x.Key, x => (object?)x.Value);

            channel.BasicPublish(_exchange, message.GetName(), false, props, message.Body.ToArray());

            _logger.LogInformation("CAP message '{0}' published, internal id '{1}'", message.GetName(), message.GetId());

            return OperateResult.Success;
        }
        catch (Exception ex)
        {
            var wrapperEx = new PublisherSentFailedException(ex.Message, ex);
            var errors = new OperateError
            {
                Code = ex.HResult.ToString(),
                Description = ex.Message
            };

            return OperateResult.Failed(wrapperEx, errors);
        }
        finally
        {
            if (channel != null) _connectionChannelPool.Return(channel);
        }
    }
}