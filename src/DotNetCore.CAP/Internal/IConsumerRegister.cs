// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Internal;

/// <summary>
/// Handler received message of subscribed.
/// </summary>
public interface IConsumerRegister : IProcessingServer
{
    bool IsHealthy();

    ValueTask ReStartAsync(bool force = false);

    /// <summary>
    /// Requests all active consumer clients to stop receiving new messages from the broker, while keeping their
    /// connections open so already-consumed messages can be committed or rejected during a graceful shutdown.
    /// </summary>
    Task StopReceivingAsync();

    /// <summary>
    /// Waits, up to <paramref name="timeout"/>, for received-message callbacks currently in flight to finish.
    /// In synchronous mode this covers the subscriber execution itself; in parallel mode the callbacks return
    /// quickly after buffering and the execution is drained separately by the dispatcher.
    /// </summary>
    Task WaitForInflightMessagesAsync(TimeSpan timeout);
}