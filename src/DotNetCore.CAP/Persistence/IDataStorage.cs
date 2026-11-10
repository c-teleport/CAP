// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;

namespace DotNetCore.CAP.Persistence;

public interface IDataStorage
{
    Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default);

    Task ReleaseLockAsync(string key, string instance, CancellationToken token = default);

    Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default);

    Task ChangePublishStateToDelayedAsync(string[] ids);

    /// <summary>
    /// Moves the given received messages to the <see cref="StatusName.RetryImmediately"/> status so the retry
    /// processor re-executes them on its next poll, bypassing the <c>FallbackWindowLookbackSeconds</c> window.
    /// Used during graceful shutdown for consumed messages that could not finish executing.
    /// </summary>
    Task ChangeReceiveStateToImmediateRetryAsync(string[] ids);

    /// <summary>
    /// Grants the caller a lease on a received message about to be re-dispatched: the row is moved to
    /// <see cref="StatusName.Scheduled"/> with <c>LeasedUntil = now + lease</c>, but only if its status is still
    /// <see cref="MediumMessage.StatusName"/> and no other retry run holds a valid lease. Returns false when the row
    /// changed underneath, in which case the caller must not dispatch it.
    /// </summary>
    Task<bool> TryLeaseReceivedMessageAsync(MediumMessage message, TimeSpan lease);

    Task ChangePublishStateAsync(MediumMessage message, StatusName state, object? transaction = null);

    Task ChangeReceiveStateAsync(MediumMessage message, StatusName state);

    Task<MediumMessage> StoreMessageAsync(string name, Message content, object? transaction = null);

    Task StoreReceivedExceptionMessageAsync(string name, string group, string content);

    Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message content);

    Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default);

    Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds);

    Task ScheduleMessagesOfDelayedAsync(Func<object, IEnumerable<MediumMessage>, Task> scheduleTask, CancellationToken token = default);

    Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds);

    Task<int> DeleteReceivedMessageAsync(long id);

    Task<int> DeletePublishedMessageAsync(long id);

    //dashboard api
    IMonitoringApi GetMonitoringApi();
}
