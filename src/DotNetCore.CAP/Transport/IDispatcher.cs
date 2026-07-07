// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Persistence;

namespace DotNetCore.CAP.Transport;

public interface IDispatcher : IProcessingServer
{
    ValueTask EnqueueToPublish(MediumMessage message);

    /// <summary>
    /// Enqueues a received message for subscriber execution.
    /// </summary>
    /// <returns>
    /// <c>true</c> if the message was accepted (buffered for parallel execution, or executed inline);
    /// <c>false</c> if it could not be accepted because CAP is shutting down. When <c>false</c>, the caller
    /// should reject the transport message so the broker can redeliver it.
    /// </returns>
    ValueTask<bool> EnqueueToExecute(MediumMessage message, ConsumerExecutorDescriptor? descriptor = null);

    Task EnqueueToScheduler(MediumMessage message, DateTime publishTime, object? transaction = null);

    /// <summary>
    /// Stops accepting new received messages and waits, up to <paramref name="grace"/>, for already-consumed
    /// messages (buffered and in-flight) to finish executing. Any messages that cannot finish within the window
    /// are moved to the <see cref="StatusName.RetryImmediately"/> status when immediate-retry-on-shutdown is enabled.
    /// </summary>
    Task DrainReceivedAsync(TimeSpan grace);
}